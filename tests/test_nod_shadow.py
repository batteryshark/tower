"""nod-answer-recorded (shadow and live), tower nod reconcile/pending, and user/password bus auth."""

import asyncio
import io
import json
import os
import subprocess
import uuid

import pytest
from aiohttp import web

from tower import bus, cli, events, kit, nod
from tower.host import BUILTIN_DIR, Host
from tower.waker import message_id_for

from conftest import NATS_SERVER, _free_port
from test_kit import FakeWaker, dead_letters, wait_for

NAME = "nod-answer-recorded"
NOD_TOKEN = "nod-detail-test-token-7f3a"
SECRET = "SECRET-ANSWER-TEXT"
ORIGIN = {"session": "ses_origin", "directory": "/tmp/somewhere", "reference": "ref-1"}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("TOWER_NOD_MODE", "TOWER_NOD_URL", "TOWER_NOD_TOKEN_FILE", "TOWER_NATS_USER",
                 "TOWER_NATS_PASSWORD_FILE", "TOWER_CONFIG", "TOWER_NOD_TRUSTED_SOURCE"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
async def fake_detail():
    """A fake Nod detail route. details[event_id] is a body, or an int status; hits counts authorized GETs."""
    state = {"details": {}, "hits": 0, "auth_failures": 0, "delay": 0}

    async def detail(request):
        if request.headers.get("Authorization") != f"Bearer {NOD_TOKEN}":
            state["auth_failures"] += 1
            return web.json_response({"error": "unauthorized"}, status=401)
        state["hits"] += 1
        if state["delay"]:
            await asyncio.sleep(state["delay"])
        value = state["details"].get(request.match_info["eid"], 404)
        if isinstance(value, int):
            return web.json_response({"error": "x"}, status=value)
        return web.json_response(value)

    app = web.Application()
    app.router.add_get("/api/v1/integrations/events/{eid}", detail)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    state["url"] = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    yield state
    await runner.cleanup()


def answer_event(rid="req_1", revision=2, status="resolved", id=None, source="nod/node-1"):
    eid = id or str(uuid.uuid4())
    data = {"schema_version": 1, "request_id": rid, "revision": revision, "aggregate_status": status,
            "decision_resolution": "shared", "decision_id": eid}
    return events.build("nod.answer.recorded", data, source=source, subject=rid, id=eid)


def detail_for(event, origin=ORIGIN):
    return {"envelope": event, "channel_id": "default", "created_by_issuer_token_id": "iss_1", "origin": origin,
            "decision": {"option_id": "yes", "text": SECRET}}


async def start_host(temp_bus, tmp_path, fake, mode, *, waker=None, fast=False, state=None):
    url, token_file = temp_bus
    token = tmp_path / "nod.token"
    token.write_text(NOD_TOKEN + "\n")
    handlers = tmp_path / "handlers"
    handlers.mkdir(exist_ok=True)
    source = (BUILTIN_DIR / "nod_answer_recorded.py").read_text()
    if fast:
        source = source.replace("BACKOFF = [5, 30, 120, 600]", "BACKOFF = [0]")
    (handlers / "nod_answer_recorded.py").write_text(source)
    logs = io.StringIO()
    config = {NAME: {"mode": mode, "url": fake["url"], "token_file": str(token)}}
    host = Host([handlers], url=url, token_file=token_file, config=config, port=None, log=kit.Log("host", logs),
                state_root=state or tmp_path / "state", waker=waker or FakeWaker())
    await host.start()
    host.test_logs = logs
    return host


COMMAND_NAMES = ("chief-nod", "nod-cli", "tower nod", "tower ", "curl", "show ", "`", "$ ", "/api/")


def assert_no_command_names(text):
    lowered = text.lower()
    for name in COMMAND_NAMES:
        assert name not in lowered, name


def test_wake_text_names_no_command():
    assert_no_command_names(nod.wake_text(answer_event(rid="req_x"), FakeWaker()))


def stats(host):
    return host.stats[NAME]


async def publish(host, event, msg_id=None):
    if msg_id is None:
        return await bus.publish(host.js, event)
    return await host.js.publish(bus.subject_for(event["type"]), events.encode(event), headers={"Nats-Msg-Id": msg_id})


async def test_shadow_computes_but_never_wakes(temp_bus, tmp_path, fake_detail):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_detail, "shadow", waker=waker)
    try:
        event = answer_event()
        fake_detail["details"][event["id"]] = detail_for(event)
        await publish(host, event)

        async def done():
            return stats(host).handled == 1
        await wait_for(done)
        assert waker.calls == []
        row = kit.shared_ledger(tmp_path / "state").bind(NAME).get(event["source"], event["id"], namespace="shadow")
        assert row["outcome"] == "handled"
        assert row["info"]["message_id"] == message_id_for(event["id"], salt=NAME)
        assert not kit.shared_ledger(tmp_path / "state").bind(NAME).seen(event["source"], event["id"], namespace="live")
        logs = host.test_logs.getvalue()
        assert "shadow wake (not sent)" in logs and "shadow pairing" in logs
        assert NOD_TOKEN not in logs and SECRET not in logs
    finally:
        await host.stop()


async def test_shadow_pairs_with_watcher_answer(temp_bus, tmp_path, fake_detail):
    host = await start_host(temp_bus, tmp_path, fake_detail, "shadow")
    try:
        event = answer_event(rid="req_pair")
        fake_detail["details"][event["id"]] = detail_for(event)
        await publish(host, event)
        watcher = events.build("nod.answer.received", {"request_id": "req_pair", "origin": ORIGIN},
                               source="tower/watcher/nod", subject="req_pair", id="nod-answer-abc")
        await publish(host, watcher)

        async def done():
            return stats(host).handled == 2
        await wait_for(done)
        pairs = [json.loads(line) for line in host.test_logs.getvalue().splitlines() if '"shadow pairing"' in line]
        assert pairs[-1]["matched"] is True
        assert pairs[-1]["request_id"] == "req_pair" and pairs[-1]["revision"] == 2
        assert pairs[-1]["recorded_event_id"] == event["id"]
        assert pairs[-1]["watcher_event_ids"] == ["nod-answer-abc"]
    finally:
        await host.stop()


async def test_live_wakes_once_without_content_and_survives_restart(temp_bus, tmp_path, fake_detail):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_detail, "live", waker=waker)
    event = answer_event(rid="req_live", revision=3, status="pending")
    fake_detail["details"][event["id"]] = detail_for(event)
    try:
        await publish(host, event)

        async def done():
            return stats(host).handled == 1
        await wait_for(done)
    finally:
        await host.stop()
    assert len(waker.calls) == 1
    kind, session, text, kwargs = waker.calls[0]
    assert (kind, session, kwargs["directory"]) == ("message", "ses_origin", "/tmp/somewhere")
    assert kwargs["message_id"] == message_id_for(event["id"], salt=NAME)
    for needle in ("req_live", '"revision": 3', '"aggregate_status": "pending"', event["id"],
                   "your own authorised Nod route"):
        assert needle in text
    assert_no_command_names(text)
    for forbidden in (SECRET, NOD_TOKEN, "yes", "iss_1", "ref-1"):
        assert forbidden not in text
    assert NOD_TOKEN not in host.test_logs.getvalue()

    # A fresh host on the same state: the same event again (past the bus duplicate window) is not woken twice.
    host = await start_host(temp_bus, tmp_path, fake_detail, "live", waker=waker)
    try:
        await publish(host, event, msg_id=event["id"] + "-again")

        async def dropped():
            return stats(host).dropped == 1
        await wait_for(dropped)
        assert len(waker.calls) == 1
        assert "already handled" in host.test_logs.getvalue()
    finally:
        await host.stop()


async def test_not_found_and_no_origin_drop(temp_bus, tmp_path, fake_detail):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_detail, "live", waker=waker)
    try:
        missing = answer_event(rid="req_404")
        no_origin = answer_event(rid="req_noorigin")
        fake_detail["details"][no_origin["id"]] = detail_for(no_origin, origin=None)
        await publish(host, missing)
        await publish(host, no_origin)

        async def done():
            return stats(host).dropped == 2
        await wait_for(done)
        assert waker.calls == []
        logs = host.test_logs.getvalue()
        assert "detail not found (404)" in logs and "no origin" in logs
    finally:
        await host.stop()


async def test_server_errors_retry_then_dead_letter(temp_bus, tmp_path, fake_detail):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_detail, "live", waker=waker, fast=True)
    try:
        event = answer_event(rid="req_500")
        fake_detail["details"][event["id"]] = 503
        await publish(host, event)

        async def done():
            return stats(host).dead_lettered == 1
        await wait_for(done, timeout=20)
        assert stats(host).retried == 4
        assert fake_detail["hits"] == 5
        letters = await dead_letters(host.js, NAME)
        assert "gave up after 5 attempts" in letters[0]["data"]["error"]
        assert waker.calls == []
        assert NOD_TOKEN not in host.test_logs.getvalue()
    finally:
        await host.stop()


async def test_mismatched_detail_dead_letters_at_once(temp_bus, tmp_path, fake_detail):
    host = await start_host(temp_bus, tmp_path, fake_detail, "live")
    try:
        event = answer_event(rid="req_forged")
        other = dict(detail_for(event))
        other["envelope"] = {**event, "data": {**event["data"], "request_id": "req_other"}}
        fake_detail["details"][event["id"]] = other
        await publish(host, event)

        async def done():
            return stats(host).dead_lettered == 1
        await wait_for(done)
        assert "data.request_id differs" in (await dead_letters(host.js, NAME))[0]["data"]["error"]
    finally:
        await host.stop()


def test_fetch_timeout_is_retryable(tmp_path):
    import socket

    mod = nod
    token = tmp_path / "t"
    token.write_text(NOD_TOKEN)
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)  # accepts the connection, never answers
    try:
        with pytest.raises(mod.FetchError) as info:
            mod.fetch_detail(f"http://127.0.0.1:{server.getsockname()[1]}", str(token), "e1", timeout=0.3)
        assert info.value.status is None and NOD_TOKEN not in str(info.value)
    finally:
        server.close()


class ReconcilingWaker(FakeWaker):
    """Like the real waker after a 409 whose id it found already admitted in the session."""

    async def message_session(self, session_id, text, **kwargs):
        self.calls.append(("message", session_id, text, kwargs))
        return type("Admission", (), {"message_id": kwargs["message_id"], "session_id": session_id,
                                      "created": False, "reconciled": True})()


async def test_409_already_admitted_marks_ledger(temp_bus, tmp_path, fake_detail):
    host = await start_host(temp_bus, tmp_path, fake_detail, "live", waker=ReconcilingWaker())
    try:
        event = answer_event(rid="req_409")
        fake_detail["details"][event["id"]] = detail_for(event)
        await publish(host, event)

        async def done():
            return stats(host).handled == 1
        await wait_for(done)
        row = kit.shared_ledger(tmp_path / "state").bind(NAME).get(event["source"], event["id"], namespace="live")
        assert row["outcome"] == "already_admitted"
        assert "origin already admitted" in host.test_logs.getvalue()
    finally:
        await host.stop()


async def test_trusted_source_drops_others(temp_bus, tmp_path, fake_detail, monkeypatch):
    monkeypatch.setenv("TOWER_NOD_TRUSTED_SOURCE", "nod/node-1")
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_detail, "live", waker=waker)
    try:
        good = answer_event(rid="req_good", source="nod/node-1")
        forged = answer_event(rid="req_forged_src", source="nod/other")
        for event in (good, forged):
            fake_detail["details"][event["id"]] = detail_for(event)
            await publish(host, event)

        async def done():
            return stats(host).handled == 1 and stats(host).dropped == 1
        await wait_for(done)
        assert len(waker.calls) == 1 and fake_detail["hits"] == 1
        assert "is not the trusted Nod source" in host.test_logs.getvalue()
    finally:
        await host.stop()


async def test_catch_up_runs_handler_shadow_by_default(tmp_path, fake_detail):
    token = tmp_path / "nod.token"
    token.write_text(NOD_TOKEN)
    events_ = [answer_event(rid=f"req_c{i}") for i in range(3)]
    for event in events_:
        fake_detail["details"][event["id"]] = detail_for(event)
    page = {"events": [{"seq": i + 1, "envelope": e} for i, e in enumerate(events_)], "next_cursor": 3,
            "high_water": 3}
    report = nod.reconcile(0, 100, state_root=tmp_path, fetch=lambda a, l, t: page)
    assert len(report["missing"]) == 3
    config = {NAME: {"url": fake_detail["url"], "token_file": str(token)}}
    waker, logs = FakeWaker(), io.StringIO()
    out = await nod.deliver(report["missing"], report["envelopes"], 2, "shadow", state_root=tmp_path, waker=waker,
                            log=kit.Log("t", logs), config=config)
    assert [(o["seq"], o["outcome"]) for o in out] == [(2, "handled"), (3, "handled")]
    assert waker.calls == [] and "shadow wake (not sent)" in logs.getvalue()
    assert os.environ.get("TOWER_NOD_MODE") is None
    again = nod.reconcile(0, 100, state_root=tmp_path, fetch=lambda a, l, t: page)
    assert [m["seq"] for m in again["missing"]] == [1]  # shadow-marked ones are no longer missing

    out = await nod.deliver(again["missing"], again["envelopes"], 0, "live", state_root=tmp_path, waker=waker,
                            log=kit.Log("t", logs), config=config)
    assert out[0]["outcome"] == "handled" and len(waker.calls) == 1
    assert NOD_TOKEN not in logs.getvalue()


# -- tower nod ---------------------------------------------------------------


def test_reconcile_reports_missing_answers_only(tmp_path):
    book = kit.shared_ledger(tmp_path).bind(NAME)
    book.record("nod/n", "e-live", namespace="live", state="handled")
    book.record("nod/n", "e-shadow", namespace="shadow", state="dropped:no origin")

    def env(seq, eid, type="nod.answer.recorded"):
        return {"seq": seq, "envelope": {"id": eid, "source": "nod/n", "type": type,
                                         "data": {"request_id": f"r{seq}", "revision": seq}}}
    pages = {0: {"events": [env(1, "e-live"), env(2, "x-created", "nod.request.created")], "next_cursor": 2,
                 "high_water": 5},
             2: {"events": [env(3, "e-shadow"), env(4, "e-missing")], "next_cursor": 5, "high_water": 5}}
    calls = []

    def fetch(after, limit, through):
        calls.append((after, limit, through))
        return pages[after]
    report = nod.reconcile(0, 2, state_root=tmp_path, fetch=fetch)
    assert calls == [(0, 2, None), (2, 2, 5)]
    assert report["scanned"] == 4 and report["answers"] == 3 and report["next_cursor"] == 5
    assert report["missing"] == [{"seq": 4, "id": "e-missing", "request_id": "r4", "revision": 4}]


def test_reconcile_empty_scoped_page_does_not_stall(tmp_path):
    report = nod.reconcile(10, 100, state_root=tmp_path,
                           fetch=lambda a, l, t: {"events": [], "next_cursor": 10, "high_water": 10})
    assert report == {"scanned": 0, "answers": 0, "missing": [], "next_cursor": 10, "envelopes": {}}


def test_pending_lists_open_ids_only(tmp_path):
    path = tmp_path / "nod-watcher" / "requests.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"b": {"state": "open", "origin": ORIGIN}, "a": {"state": "open"},
                                "c": {"state": "answered"}}))
    assert nod.pending(tmp_path) == ["a", "b"]
    assert nod.pending(tmp_path / "nowhere") == []


def test_cli_parses_nod_commands():
    args = cli.build_parser().parse_args(["nod", "reconcile", "--after", "7", "--limit", "50"])
    assert (args.nod_command, args.after, args.limit) == ("reconcile", 7, 50)
    assert cli.build_parser().parse_args(["nod", "pending"]).nod_command == "pending"
    args = cli.build_parser().parse_args(["nod", "reconcile", "--after", "0", "--deliver-from-seq", "5"])
    assert (args.deliver_from_seq, args.mode) == (5, "shadow")


# -- bus auth -------------------------------------------------------------------


@pytest.fixture
def user_bus(tmp_path):
    if not NATS_SERVER:
        pytest.skip("nats-server not installed")
    port = _free_port()
    proc = subprocess.Popen([NATS_SERVER, "-a", "127.0.0.1", "-p", str(port), "--user", "nod", "--pass", "pw-123"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    import socket
    import time
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    yield f"nats://127.0.0.1:{port}"
    proc.terminate()
    proc.wait(timeout=10)


async def test_connect_with_user_password_file(user_bus, tmp_path, monkeypatch):
    password = tmp_path / "nod.password"
    password.write_text("pw-123\n")
    token = tmp_path / "nats.token"
    token.write_text("not-used\n")
    monkeypatch.setenv("TOWER_NATS_USER", "nod")
    monkeypatch.setenv("TOWER_NATS_PASSWORD_FILE", str(password))
    nc = await bus.connect(user_bus, str(token))
    assert nc.is_connected
    await nc.close()


async def test_token_stays_default_and_user_needs_password(user_bus, tmp_path, monkeypatch):
    token = tmp_path / "nats.token"
    token.write_text("wrong\n")
    with pytest.raises(Exception):
        await asyncio.wait_for(bus.connect(user_bus, str(token), allow_reconnect=False), 10)
    monkeypatch.setenv("TOWER_NATS_USER", "nod")
    with pytest.raises(ValueError, match="TOWER_NATS_PASSWORD_FILE"):
        await bus.connect(user_bus, str(token))
