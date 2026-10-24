"""Pre-live fixes: the cross-mode cutover fence, and a wake built only from the authenticated detail."""

import asyncio
import copy
import io

import pytest

from tower import bus, events, kit, nod
from tower.host import BUILTIN_DIR, Host
from tower.waker import message_id_for

from test_kit import FakeWaker, dead_letters, wait_for
from test_nod_recovery import NOD_TOKEN, ORIGIN, answer, clean, env_for, fake_nod, row  # noqa: F401

FENCED = "dropped:pre-cutover-shadow"


async def start_host(temp_bus, tmp_path, fake, mode, waker):
    url, token_file = temp_bus
    handlers = tmp_path / "handlers"
    handlers.mkdir(exist_ok=True)
    (handlers / "nod_answer_recorded.py").write_text((BUILTIN_DIR / "nod_answer_recorded.py").read_text())
    token = tmp_path / "nod.token"
    token.write_text(NOD_TOKEN)
    config = {nod.HANDLER: {"mode": mode, "url": fake["url"], "token_file": str(token)}}
    logs = io.StringIO()
    host = Host([handlers], url=url, token_file=token_file, config=config, port=None, log=kit.Log("host", logs),
                state_root=tmp_path / "state", waker=waker)
    await host.start()
    host.test_logs = logs
    return host


class FakeMsg:
    """A redelivery of a stored stream message, as the host sees it."""

    def __init__(self, data, subject, seq, delivered=2):
        self.data, self.subject, self.acked = data, subject, None
        meta = type("Meta", (), {})()
        meta.num_delivered = delivered
        meta.sequence = type("Seq", (), {"stream": seq})()
        self.metadata = meta

    async def ack(self):
        self.acked = "ack"

    async def nak(self, delay=None):
        self.acked = "nak"

    async def term(self):
        self.acked = "term"


# -- fix 1: cutover fence -------------------------------------------------------------


async def test_shadow_evaluated_event_never_wakes_after_flip(temp_bus, tmp_path, fake_nod):
    waker = FakeWaker()
    shadow_host = await start_host(temp_bus, tmp_path, fake_nod, "shadow", waker)
    event = answer(fake_nod, rid="req_fence")
    try:
        ack = await bus.publish(shadow_host.js, event)

        async def evaluated():
            return (row(tmp_path, event, "shadow") or {}).get("outcome") == "handled"
        await wait_for(evaluated)
    finally:
        await shadow_host.stop()
    assert waker.calls == []

    live = await start_host(temp_bus, tmp_path, fake_nod, "live", waker)  # the flip
    try:
        spec = next(s for s in live.specs if s.name == nod.HANDLER)
        ctx = live._context(spec.name)
        # (a) the same bus message redelivered
        msg = FakeMsg(events.encode(event), "ev.nod.answer.recorded", ack.seq)
        assert await live.process(spec, ctx, msg) == "dropped" and msg.acked == "ack"
        assert row(tmp_path, event)["outcome"] == FENCED
        # (c) a duplicate with a different Nats-Msg-Id, after the dedupe window
        await live.js.publish("ev.nod.answer.recorded", events.encode(event), headers={"Nats-Msg-Id": "later-copy"})

        async def dropped():
            return live.stats[nod.HANDLER].dropped >= 2
        await wait_for(dropped)
        # (b) the same event from catch-up
        env = env_for(tmp_path, fake_nod, mode="live", waker=waker)
        result = await nod.run_catchup(env)
        assert result["result"] == "caught_up" and result["terminal"] == 1
        assert waker.calls == []
        rows = kit.shared_ledger(tmp_path / "state").bind(nod.HANDLER).by_event(event["id"], namespace="live")
        assert len(rows) == 1 and rows[0]["outcome"] == FENCED
        assert nod.status(tmp_path / "state")["counts"]["live"] == {"dropped": 1}

        # an event first seen after the flip still wakes once
        fresh = answer(fake_nod, rid="req_after")
        await bus.publish(live.js, fresh)

        async def woke():
            return (row(tmp_path, fresh) or {}).get("outcome") == "handled"
        await wait_for(woke)
        await nod.run_catchup(env)
        assert [c[3]["message_id"] for c in waker.calls] == [message_id_for(fresh["id"], salt=nod.HANDLER)]
    finally:
        await live.stop()

    # the one deliberate override: tower nod retry <id> --mode live
    logs = io.StringIO()
    cli = nod.cli_env("live", tmp_path / "state", waker, kit.Log("cli", logs),
                      {nod.HANDLER: {"url": fake_nod["url"], "token_file": str(tmp_path / "nod.token")}})
    outcome = await nod.retry(event["id"], cli)
    assert outcome.state == "handled" and len(waker.calls) == 2
    assert "operator override of the pre-cutover shadow fence" in logs.getvalue()
    assert row(tmp_path, event, "shadow")["outcome"] == "handled"  # shadow history untouched


async def test_retry_override_works_without_a_live_row(tmp_path, fake_nod):
    event = answer(fake_nod, rid="req_only_shadow")
    await nod.run_catchup(env_for(tmp_path, fake_nod, mode="shadow"))
    waker = FakeWaker()
    cli = nod.cli_env("live", tmp_path / "state", waker, kit.Log("cli", io.StringIO()),
                      {nod.HANDLER: {"url": fake_nod["url"], "token_file": str(tmp_path / "nod.token")}})
    assert (await nod.retry(event["id"], cli)).state == "handled" and len(waker.calls) == 1
    with pytest.raises(nod.NodError):
        await nod.retry(event["id"], cli)  # now handled in live: nothing to override


async def test_fence_ignores_non_terminal_shadow_rows(tmp_path, fake_nod):
    event = answer(fake_nod, rid="req_retrying", detail=503)
    await nod.run_catchup(env_for(tmp_path, fake_nod, mode="shadow"))
    assert row(tmp_path, event, "shadow")["outcome"].startswith("retrying:")
    fake_nod["details"][event["id"]] = {"envelope": event, "channel_id": "c", "created_by_issuer_token_id": None,
                                        "origin": ORIGIN, "decision": None}
    env = env_for(tmp_path, fake_nod, mode="live")
    await nod.run_catchup(env)
    assert row(tmp_path, event)["outcome"] == "handled" and len(env.waker.calls) == 1


# -- fix 2: wake only from the authenticated detail ------------------------------------------


@pytest.mark.parametrize("field,change", [
    ("data.aggregate_status", lambda e: e["data"].update(aggregate_status="pending")),
    ("data.decision_id", lambda e: e["data"].update(decision_id="forged-decision")),
    ("subject", lambda e: e.update(subject="req_other")),
])
async def test_bus_field_differing_from_detail_dead_letters(temp_bus, tmp_path, fake_nod, field, change):
    waker = FakeWaker()
    host = await start_host(temp_bus, tmp_path, fake_nod, "live", waker)
    try:
        genuine = answer(fake_nod, rid="req_mm")
        forged = copy.deepcopy(genuine)
        change(forged)  # same id and revision, so it passes every check before the detail fetch
        await bus.publish(host.js, forged)

        async def done():
            return host.stats[nod.HANDLER].dead_lettered == 1 and row(tmp_path, forged) is not None
        await wait_for(done)
        assert waker.calls == []
        assert row(tmp_path, forged)["outcome"] == f"dead_lettered:detail-mismatch:{field}"
        assert (await dead_letters(host.js, nod.HANDLER))[0]["data"]["error"].endswith(f"detail-mismatch:{field}")
    finally:
        await host.stop()


async def test_mismatch_is_the_same_on_every_path(tmp_path, fake_nod):
    genuine = answer(fake_nod, rid="req_paths")
    forged = copy.deepcopy(genuine)
    forged["data"]["aggregate_status"] = "pending"
    fake_nod["journal"][-1] = forged  # catch-up reads the altered copy; the detail stays genuine
    env = env_for(tmp_path, fake_nod, mode="live")
    await nod.run_catchup(env)
    assert row(tmp_path, forged)["outcome"] == "dead_lettered:detail-mismatch:data.aggregate_status"
    report = nod.reconcile(0, 100, state_root=tmp_path / "state",
                           fetch=lambda a, l, t: {"events": [{"seq": 1, "envelope": forged}], "next_cursor": 1,
                                                  "high_water": 1})
    assert report["missing"] == []  # dead_lettered is terminal
    out = await nod.deliver([{"seq": 1, "id": forged["id"]}], {forged["id"]: forged}, 0, "shadow",
                            state_root=tmp_path / "state", waker=env.waker,
                            config={nod.HANDLER: {"url": fake_nod["url"], "token_file": env.cfg.token_file}})
    assert out[0]["outcome"] == "dead_lettered:detail-mismatch:data.aggregate_status"
    assert env.waker.calls == []


async def test_matching_event_wakes_with_detail_values(tmp_path, fake_nod):
    event = answer(fake_nod, rid="req_match", revision=4, status="pending")
    detail_envelope = copy.deepcopy(fake_nod["details"][event["id"]]["envelope"])
    env = env_for(tmp_path, fake_nod, mode="live")
    await nod.run_catchup(env)
    (kind, session, text, kwargs), = env.waker.calls
    assert (session, kwargs["directory"]) == (ORIGIN["session"], ORIGIN["directory"])
    assert text == nod.wake_text(detail_envelope, env.waker)
    for value in ('"revision": 4', '"aggregate_status": "pending"', f'"decision_id": "{event["id"]}"', "req_match"):
        assert value in text
