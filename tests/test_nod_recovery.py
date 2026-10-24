"""Nod journal catch-up, durable cursor, failure state and operator retry (fake Nod, throwaway nats-server)."""

import asyncio
import io
import json
import uuid

import pytest
from aiohttp import web

from tower import bus, events, kit, nod
from tower.host import BUILTIN_DIR, Host
from tower.waker import message_id_for

from test_kit import FakeWaker, dead_letters, wait_for

NOD_TOKEN = "nod-recovery-test-token-91c2"
ORIGIN = {"session": "ses_origin", "directory": "/tmp/somewhere", "reference": None}


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    for name in ("TOWER_NOD_MODE", "TOWER_NOD_URL", "TOWER_NOD_TOKEN_FILE", "TOWER_NOD_TRUSTED_SOURCE",
                 "TOWER_NOD_CATCHUP", "TOWER_CONFIG"):
        monkeypatch.delenv(name, raising=False)
    kit._LEDGERS.clear()
    yield
    for ledger in kit._LEDGERS.values():
        ledger.close()
    kit._LEDGERS.clear()


@pytest.fixture
async def fake_nod():
    """Detail and journal list. journal: list of envelopes (seq = index + 1). details[id]: body or status."""
    state = {"journal": [], "details": {}, "list_status": None, "list_hits": 0, "detail_hits": 0}

    def authorized(request):
        return request.headers.get("Authorization") == f"Bearer {NOD_TOKEN}"

    async def detail(request):
        if not authorized(request):
            return web.json_response({}, status=401)
        state["detail_hits"] += 1
        value = state["details"].get(request.match_info["eid"], 404)
        if isinstance(value, int):
            return web.json_response({"error": "x"}, status=value)
        return web.json_response(value)

    async def listing(request):
        if not authorized(request):
            return web.json_response({}, status=401)
        state["list_hits"] += 1
        if state["list_status"]:
            return web.json_response({"error": "x"}, status=state["list_status"])
        high = len(state["journal"])
        after = int(request.query.get("after", 0))
        limit = int(request.query.get("limit", 100))
        through = int(request.query.get("through", high))
        if after < 0 or not 1 <= limit <= 500 or through < after or through > high:
            return web.json_response({"error": "bad cursor"}, status=400)
        rows = [{"seq": i + 1, "envelope": e} for i, e in enumerate(state["journal"]) if after < i + 1 <= through]
        rows = rows[:limit]
        return web.json_response({"events": rows, "next_cursor": rows[-1]["seq"] if rows else through,
                                  "high_water": through})

    app = web.Application()
    app.router.add_get("/api/v1/integrations/events", listing)
    app.router.add_get("/api/v1/integrations/events/{eid}", detail)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    state["url"] = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    yield state
    await runner.cleanup()


def answer(fake, rid="req_1", revision=1, *, detail=True, status="resolved", origin=ORIGIN):
    """Add one answer to the fake journal (and its detail); returns the envelope."""
    eid = str(uuid.uuid4())
    data = {"schema_version": 1, "request_id": rid, "revision": revision, "aggregate_status": status,
            "decision_resolution": "per_user", "decision_id": eid}
    event = events.build("nod.answer.recorded", data, source="nod/node-1", subject=rid, id=eid)
    fake["journal"].append(event)
    if detail is True:
        fake["details"][eid] = {"envelope": event, "channel_id": "c", "created_by_issuer_token_id": None,
                                "origin": origin, "decision": {"text": "SECRET"}}
    elif isinstance(detail, int):
        fake["details"][eid] = detail
    return event


def env_for(tmp_path, fake, mode="live", waker=None, js=None, **cfg):
    token = tmp_path / "nod.token"
    token.write_text(NOD_TOKEN)
    settings = nod.Settings(mode=mode, url=fake["url"], token_file=str(token), catchup=True, **cfg)
    return nod.Env(settings, waker or FakeWaker(), kit.Log("t", io.StringIO()), tmp_path / "state", js)


def cursor(tmp_path):
    return kit.shared_ledger(tmp_path / "state").get_value(nod.CURSOR_KEY)


def row(tmp_path, event, mode="live"):
    return kit.shared_ledger(tmp_path / "state").bind(nod.HANDLER).get(event["source"], event["id"], namespace=mode)


# -- catch-up ---------------------------------------------------------------------


async def test_catchup_recovers_event_the_stream_lost(tmp_path, fake_nod):
    lost = answer(fake_nod, rid="req_lost")  # in Nod's journal, never on the bus
    env = env_for(tmp_path, fake_nod)
    result = await nod.run_catchup(env)
    assert result["result"] == "caught_up" and result["cursor"] == 1 and cursor(tmp_path) == 1
    assert len(env.waker.calls) == 1
    assert env.waker.calls[0][3]["message_id"] == message_id_for(lost["id"], salt=nod.HANDLER)
    assert row(tmp_path, lost)["outcome"] == "handled"
    assert row(tmp_path, lost)["info"]["via"] == "catch-up"
    again = await nod.run_catchup(env)  # nothing new: no second wake
    assert again["processed"] == 0 and len(env.waker.calls) == 1


async def test_cursor_stops_before_non_terminal_and_survives_restart(tmp_path, fake_nod):
    first = answer(fake_nod, rid="req_a")
    stuck = answer(fake_nod, rid="req_b", detail=503)
    later = answer(fake_nod, rid="req_c")
    env = env_for(tmp_path, fake_nod)
    result = await nod.run_catchup(env)
    assert result["result"] == "blocked" and result["cursor"] == 1
    assert row(tmp_path, stuck)["outcome"].startswith("retrying:") and row(tmp_path, stuck)["attempts"] == 1
    assert row(tmp_path, later)["outcome"] == "handled"  # out of order is fine; the cursor still waits
    assert len(env.waker.calls) == 2

    for ledger in kit._LEDGERS.values():  # a host restart: a fresh connection to the same file
        ledger.close()
    kit._LEDGERS.clear()
    assert cursor(tmp_path) == 1
    fake_nod["details"][stuck["id"]] = {"envelope": stuck, "channel_id": "c", "created_by_issuer_token_id": None,
                                        "origin": ORIGIN, "decision": None}
    env = env_for(tmp_path, fake_nod, waker=env.waker)
    result = await nod.run_catchup(env)
    assert result["result"] == "caught_up" and cursor(tmp_path) == 3
    assert row(tmp_path, stuck)["outcome"] == "handled" and row(tmp_path, stuck)["attempts"] == 2
    assert [c[3]["message_id"] for c in env.waker.calls].count(message_id_for(first["id"], salt=nod.HANDLER)) == 1
    assert len(env.waker.calls) == 3


async def test_catchup_dead_letters_after_max_attempts(temp_bus, tmp_path, fake_nod):
    url, token_file = temp_bus
    nc = await bus.connect(url, token_file)
    js = nc.jetstream()
    await bus.ensure_streams(js)
    try:
        bad = answer(fake_nod, rid="req_dead", detail=500)
        env = env_for(tmp_path, fake_nod, js=js)
        for attempt in range(1, nod.MAX_ATTEMPTS):
            assert (await nod.run_catchup(env))["result"] == "blocked"
            assert row(tmp_path, bad)["attempts"] == attempt
        result = await nod.run_catchup(env)
        assert result["result"] == "caught_up" and cursor(tmp_path) == 1
        assert row(tmp_path, bad)["outcome"].startswith("dead_lettered:gave up after 5 attempts")
        letters = await dead_letters(js, nod.HANDLER)
        assert letters[0]["data"]["event"]["id"] == bad["id"] and letters[0]["data"]["via"] == "catch-up"
        report = nod.status(tmp_path / "state")
        assert report["cursor"] == 1 and report["counts"]["live"] == {"dead_lettered": 1}
        assert report["attention"][0]["event_id"] == bad["id"] and report["attention"][0]["attempts"] == 5
        assert report["last_run"]["result"] == "caught_up"
    finally:
        await nc.close()


async def test_high_water_is_respected(tmp_path):
    calls = []
    journal = [{"seq": i, "envelope": {"id": f"e{i}", "type": "nod.request.created", "source": "nod/n"}}
               for i in range(1, 6)]

    def fetch(after, limit, through):
        calls.append((after, limit, through))
        high = through if through is not None else 3  # the journal grows to 5 after the first page
        rows = [r for r in journal if after < r["seq"] <= high][:limit]
        return {"events": rows, "next_cursor": rows[-1]["seq"] if rows else high, "high_water": high}

    env = nod.Env(nod.Settings(catchup=True, catchup_limit=2), FakeWaker(), kit.Log("t", io.StringIO()),
                  tmp_path / "state")
    result = await nod.run_catchup(env, fetch)
    assert calls == [(0, 2, None), (2, 2, 3)]
    assert result["cursor"] == 3 and result["high_water"] == 3 and result["result"] == "caught_up"


async def test_page_budget_per_run(tmp_path, fake_nod):
    for i in range(5):
        fake_nod["journal"].append(events.build("nod.request.created", {"request_id": f"r{i}", "revision": 1},
                                                source="nod/node-1"))
    env = env_for(tmp_path, fake_nod, catchup_limit=1, catchup_pages=2)
    assert (await nod.run_catchup(env))["result"] == "more" and cursor(tmp_path) == 2
    assert (await nod.run_catchup(env))["result"] == "more" and cursor(tmp_path) == 4
    assert (await nod.run_catchup(env))["result"] == "caught_up" and cursor(tmp_path) == 5


async def test_out_of_order_revisions_are_both_handled(tmp_path, fake_nod):
    newer = answer(fake_nod, rid="req_pu", revision=3, status="resolved")
    older = answer(fake_nod, rid="req_pu", revision=2, status="pending")
    env = env_for(tmp_path, fake_nod)
    await nod.run_catchup(env)
    ids = [c[3]["message_id"] for c in env.waker.calls]
    assert ids == [message_id_for(newer["id"], salt=nod.HANDLER), message_id_for(older["id"], salt=nod.HANDLER)]
    assert row(tmp_path, older)["outcome"] == "handled" and row(tmp_path, newer)["outcome"] == "handled"


async def test_unavailable_journal_backs_off_quietly(tmp_path, fake_nod):
    fake_nod["list_status"] = 404
    env = env_for(tmp_path, fake_nod)
    result = await nod.run_catchup(env)
    assert result["result"] == "unavailable" and "404" in result["error"] and cursor(tmp_path) is None

    logs = io.StringIO()
    ctx = kit.Context(name=nod.CATCHUP, js=None, log=kit.Log("c", logs), state_root=tmp_path / "state",
                      config={"url": fake_nod["url"], "token_file": env.cfg.token_file, "catchup": "on",
                              "catchup_interval": 0.05}, reconnected=asyncio.Event())
    task = asyncio.create_task(nod.catchup_loop(ctx))
    await asyncio.sleep(0.5)
    assert not task.done()
    task.cancel()
    lines = [json.loads(line) for line in logs.getvalue().splitlines()]
    backoffs = [line["retry_in"] for line in lines if line["msg"] == "journal unavailable, backing off"]
    assert backoffs[:3] == [0.1, 0.2, 0.4]  # doubling, no crash loop
    assert nod.status(tmp_path / "state")["last_run"]["result"] == "unavailable"


async def test_disabled_catchup_makes_no_nod_call(tmp_path, fake_nod):
    logs = io.StringIO()
    ctx = kit.Context(name=nod.CATCHUP, js=None, log=kit.Log("c", logs), state_root=tmp_path / "state",
                      config={"url": fake_nod["url"], "token_file": "/nonexistent"})
    task = asyncio.create_task(nod.catchup_loop(ctx))
    await asyncio.sleep(0.2)
    task.cancel()
    assert fake_nod["list_hits"] == 0 and '"disabled"' in logs.getvalue()
    assert nod.status(tmp_path / "state")["last_run"]["result"] == "disabled"


async def test_reconnect_triggers_a_run(tmp_path, fake_nod):
    ctx = kit.Context(name=nod.CATCHUP, js=None, log=kit.Log("c", io.StringIO()), state_root=tmp_path / "state",
                      config={"url": fake_nod["url"], "token_file": str(tmp_path / "nod.token"), "catchup": "on",
                              "catchup_interval": 3600}, waker=FakeWaker(), reconnected=asyncio.Event())
    (tmp_path / "nod.token").write_text(NOD_TOKEN)
    task = asyncio.create_task(nod.catchup_loop(ctx))
    try:
        async def ran(n):
            return fake_nod["list_hits"] >= n
        await wait_for(lambda: ran(1))
        answer(fake_nod, rid="req_rc")
        ctx.reconnected.set()
        await wait_for(lambda: ran(2))
        event = fake_nod["journal"][-1]

        async def evaluated():
            return (row(tmp_path, event, "shadow") or {}).get("outcome") == "handled"
        await wait_for(evaluated)
        assert ctx.waker.calls == []  # shadow by default: no wake
    finally:
        task.cancel()


# -- one wake across paths -----------------------------------------------------------------


class SlowWaker(FakeWaker):
    async def message_session(self, session_id, text, **kwargs):
        await asyncio.sleep(0.3)
        return await super().message_session(session_id, text, **kwargs)


async def test_bus_and_catchup_racing_wake_once(tmp_path, fake_nod):
    event = answer(fake_nod, rid="req_race")
    waker = SlowWaker()
    env = env_for(tmp_path, fake_nod, waker=waker)
    bus_path = asyncio.create_task(nod.process(event, env, via="bus"))
    await asyncio.sleep(0.05)
    with pytest.raises(nod.Busy):
        await nod.process(event, env, via="catch-up", dead_letter=nod.bus_dead_letter(env))
    assert (await bus_path).state == "handled"
    later = await nod.process(event, env, via="catch-up", dead_letter=nod.bus_dead_letter(env))
    assert later.skipped and len(waker.calls) == 1


async def start_host(temp_bus, tmp_path, fake, *, catchup=False, waker=None):
    url, token_file = temp_bus
    handlers = tmp_path / "handlers"
    handlers.mkdir(exist_ok=True)
    (handlers / "nod_answer_recorded.py").write_text((BUILTIN_DIR / "nod_answer_recorded.py").read_text())
    (handlers / "nod_catchup.py").write_text((BUILTIN_DIR / "nod_catchup.py").read_text())
    token = tmp_path / "nod.token"
    token.write_text(NOD_TOKEN)
    common = {"mode": "live", "url": fake["url"], "token_file": str(token)}
    config = {nod.HANDLER: common,
              nod.CATCHUP: {**common, "catchup": "on" if catchup else "off", "catchup_interval": 0.2}}
    logs = io.StringIO()
    host = Host([handlers], url=url, token_file=token_file, config=config, port=None, log=kit.Log("host", logs),
                state_root=tmp_path / "state", waker=waker or FakeWaker())
    await host.start()
    host.test_logs = logs
    return host


async def test_bus_and_catchup_deliver_same_event_once(temp_bus, tmp_path, fake_nod):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_nod, catchup=True, waker=waker)
    try:
        event = answer(fake_nod, rid="req_both")
        await bus.publish(host.js, event)

        async def settled():
            return cursor(tmp_path) == 1 and row(tmp_path, event) is not None
        await wait_for(settled)
        await asyncio.sleep(0.5)  # a few more catch-up runs and any redelivery
        assert len(waker.calls) == 1
        assert row(tmp_path, event)["outcome"] == "handled"
    finally:
        await host.stop()


async def test_recreated_consumer_misses_event_and_catchup_finds_it(temp_bus, tmp_path, fake_nod):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_nod, waker=waker)
    await host.stop()
    nc = await bus.connect(*temp_bus)
    js = nc.jetstream()
    await js.delete_consumer(bus.STREAM, nod.HANDLER)
    event = answer(fake_nod, rid="req_gap")
    await bus.publish(js, event)  # published while no consumer exists
    await nc.close()

    host = await start_host(temp_bus, tmp_path, fake_nod, waker=waker)  # consumer recreated, delivers NEW only
    try:
        await asyncio.sleep(0.5)
        assert row(tmp_path, event) is None and waker.calls == []
    finally:
        await host.stop()
    host = await start_host(temp_bus, tmp_path, fake_nod, catchup=True, waker=waker)
    try:
        async def found():
            return (row(tmp_path, event) or {}).get("outcome") == "handled"
        await wait_for(found)
        assert len(waker.calls) == 1
    finally:
        await host.stop()


async def test_bus_dead_letter_leaves_durable_row(temp_bus, tmp_path, fake_nod):
    host = await start_host(temp_bus, tmp_path, fake_nod)
    try:
        event = answer(fake_nod, rid="req_400", detail=400)
        await bus.publish(host.js, event)

        async def done():
            return host.stats[nod.HANDLER].dead_lettered == 1 and row(tmp_path, event) is not None
        await wait_for(done)
        assert row(tmp_path, event)["outcome"].startswith("dead_lettered:Fail: detail fetch: HTTP 400")
        assert row(tmp_path, event)["info"]["via"] == "bus"
    finally:
        await host.stop()


# -- operator retry ------------------------------------------------------------------


async def test_retry_reprocesses_dead_letter_in_shadow_by_default(tmp_path, fake_nod):
    event = answer(fake_nod, rid="req_retry", detail=403)
    env = env_for(tmp_path, fake_nod, mode="shadow")
    await nod.run_catchup(env)
    assert row(tmp_path, event, "shadow")["outcome"].startswith("dead_lettered:")
    with pytest.raises(nod.NodError, match="not dead_lettered"):
        other = answer(fake_nod, rid="req_ok")
        await nod.run_catchup(env)
        await nod.retry(other["id"], env)

    fake_nod["details"][event["id"]] = {"envelope": event, "channel_id": "c", "created_by_issuer_token_id": None,
                                        "origin": ORIGIN, "decision": None}
    cli = nod.cli_env(state_root=tmp_path / "state", waker=env.waker, config={nod.HANDLER: {
        "url": fake_nod["url"], "token_file": env.cfg.token_file}})
    assert cli.cfg.mode == "shadow"
    outcome = await nod.retry(event["id"], cli)
    assert outcome.state == "handled" and env.waker.calls == []
    stored = row(tmp_path, event, "shadow")
    assert stored["outcome"] == "handled" and stored["info"]["via"] == "retry"
    with pytest.raises(nod.NodError, match="not dead_lettered"):
        await nod.retry(event["id"], cli)
