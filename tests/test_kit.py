"""Handler kit and host against a throwaway nats-server: ack, retry, drop, fail, dead letters, broken files."""

import asyncio
import io
import json
import textwrap

import pytest

from tower import bus, events, kit
from tower.host import Host
from tower.waker import WakeError, data_block, message_id_for


class FakeWaker:
    message_id_for = staticmethod(message_id_for)
    data_block = staticmethod(data_block)
    WakeError = WakeError

    def __init__(self, error=None):
        self.calls = []
        self.error = error

    async def message_session(self, session_id, text, **kwargs):
        self.calls.append(("message", session_id, text, kwargs))
        if self.error:
            raise self.error
        return type("Admission", (), {"message_id": kwargs.get("message_id"), "session_id": session_id,
                                      "created": False})()

    async def start_session(self, agent, text, **kwargs):
        self.calls.append(("start", agent, text, kwargs))
        if self.error:
            raise self.error
        return type("Admission", (), {"message_id": kwargs.get("message_id"), "session_id": "ses_new",
                                      "created": True})()


HANDLERS = {
    "ok.py": """
        NAME = "ok"
        SUBSCRIBE = "ev.test.ok.>"
        async def handle(event, ctx):
            (ctx.state_dir / "seen").open("a").write(event["id"] + "\\n")
    """,
    "flaky.py": """
        NAME = "flaky"
        SUBSCRIBE = "ev.test.flaky.sent"
        BACKOFF = [0.1]
        async def handle(event, ctx):
            marker = ctx.state_dir / "attempts"
            n = int(marker.read_text()) if marker.exists() else 0
            marker.write_text(str(n + 1))
            if n < 2:
                raise RuntimeError("not yet")
    """,
    "always_fail.py": """
        from tower.kit import Retry
        NAME = "always-fail"
        SUBSCRIBE = "ev.test.fail.sent"
        MAX_DELIVER = 3
        BACKOFF = [0.1]
        async def handle(event, ctx):
            raise Retry("never works")
    """,
    "dropper.py": """
        from tower.kit import Drop
        NAME = "dropper"
        SUBSCRIBE = ["ev.test.drop.sent", "ev.test.ok.extra"]
        async def handle(event, ctx):
            raise Drop("not for me")
    """,
    "failer.py": """
        from tower.kit import Fail
        NAME = "failer"
        SUBSCRIBE = "ev.test.failnow.sent"
        async def handle(event, ctx):
            raise Fail("bad input")
    """,
    "waker_user.py": """
        NAME = "waker-user"
        SUBSCRIBE = "ev.test.wake.sent"
        BACKOFF = [0.1]
        async def handle(event, ctx):
            await ctx.waker.message_session("ses_x", "hi", message_id=ctx.waker.message_id_for(event["id"]))
    """,
    "watcher.py": """
        import asyncio
        NAME = "ticker"
        async def run(ctx):
            await ctx.publish("test.tick.sent", {"n": 1}, (), "tick-1")
            await asyncio.Event().wait()
    """,
    "crashy_watcher.py": """
        NAME = "crashy"
        async def run(ctx):
            raise RuntimeError("boom")
    """,
    "broken_syntax.py": "def oops(:\n",
    "no_name.py": """
        async def handle(event, ctx):
            pass
    """,
    "raises_on_import.py": """
        raise SystemExit("do not exit the host")
    """,
    "_private.py": "raise RuntimeError('never loaded')",
}


def write_handlers(directory):
    directory.mkdir()
    for name, body in HANDLERS.items():
        (directory / name).write_text(textwrap.dedent(body))


async def wait_for(predicate, timeout=10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(0.1)
    raise AssertionError("condition not met in time")


async def dead_letters(js, handler):
    try:
        info = await js.stream_info(bus.DLQ_STREAM)
    except Exception:
        return []
    out = []
    for seq in range(info.state.first_seq, info.state.last_seq + 1):
        try:
            msg = await js.get_msg(bus.DLQ_STREAM, seq)
        except Exception:
            continue
        if msg.subject == bus.dlq_subject(handler):
            out.append(json.loads(msg.data))
    return out


@pytest.fixture
async def host(temp_bus, tmp_path):
    url, token_file = temp_bus
    write_handlers(tmp_path / "handlers")
    logs = io.StringIO()
    waker = FakeWaker()
    h = Host([tmp_path / "handlers", tmp_path / "missing"], url=url, token_file=token_file, config={}, port=None,
             log=kit.Log("host", logs), state_root=tmp_path / "state", waker=waker)
    await h.start()
    h.test_logs, h.test_waker, h.test_state = logs, waker, tmp_path / "state"
    try:
        yield h
    finally:
        await h.stop()


async def pub(js, type, data=None, qualifiers=(), id=None):
    return await bus.publish(js, events.build(type, data or {}, source="tests", id=id), qualifiers)


async def test_loads_good_files_and_isolates_broken_ones(host):
    names = {s.name for s in host.specs}
    assert names == {"ok", "flaky", "always-fail", "dropper", "failer", "waker-user", "ticker", "crashy"}
    bad = {e["path"].rsplit("/", 1)[-1] for e in host.load_errors}
    assert bad == {"broken_syntax.py", "no_name.py", "raises_on_import.py"}
    assert "load failed" in host.test_logs.getvalue()


async def test_ack_and_durable_consumer(host):
    ack = await pub(host.js, "test.ok.sent", {"x": 1}, ["a"])
    seen = host.test_state / "ok" / "seen"
    await wait_for(lambda: _async(seen.exists()))
    assert seen.read_text().split() == [ack and (await _last_id(host.js, ack.seq))]
    info = await host.js.consumer_info(bus.STREAM, "ok")
    assert info.config.durable_name == "ok" and info.num_ack_pending == 0
    assert host.stats["ok"].handled == 1


async def test_retry_then_success(host):
    await pub(host.js, "test.flaky.sent")
    await wait_for(lambda: _async(host.stats["flaky"].handled == 1))
    assert host.stats["flaky"].retried == 2
    assert (host.test_state / "flaky" / "attempts").read_text() == "3"


async def test_dead_letter_after_max_deliver(host):
    ack = await pub(host.js, "test.fail.sent", {"n": 1})
    letters = await wait_for(lambda: dead_letters(host.js, "always-fail"))
    assert len(letters) == 1
    letter = letters[0]
    assert letter["type"] == "tower.dlq.recorded"
    assert letter["data"]["handler"] == "always-fail"
    assert letter["data"]["deliveries"] == 3
    assert letter["data"]["stream_seq"] == ack.seq
    assert letter["data"]["event"]["data"] == {"n": 1}
    assert "never works" in letter["data"]["error"]
    assert host.stats["always-fail"].retried == 2
    await asyncio.sleep(0.5)
    info = await host.js.consumer_info(bus.STREAM, "always-fail")
    assert info.num_ack_pending == 0 and info.num_pending == 0


async def test_drop_and_multiple_filters(host):
    await pub(host.js, "test.drop.sent")
    await pub(host.js, "test.ok.extra")
    await wait_for(lambda: _async(host.stats["dropper"].dropped == 2))
    assert await dead_letters(host.js, "dropper") == []


async def test_fail_dead_letters_immediately(host):
    await pub(host.js, "test.failnow.sent")
    letters = await wait_for(lambda: dead_letters(host.js, "failer"))
    assert letters[0]["data"]["deliveries"] == 1
    assert "bad input" in letters[0]["data"]["error"]


async def test_invalid_envelope_dead_lettered(host):
    await host.js.publish("ev.test.ok.raw", b"not json at all")
    letters = await wait_for(lambda: dead_letters(host.js, "ok"))
    assert letters[0]["data"]["event"] == {"raw": "not json at all"}
    assert "invalid event" in letters[0]["data"]["error"]


async def test_wake_errors(host):
    host.test_waker.error = WakeError("bad_target", "session not found")
    await pub(host.js, "test.wake.sent")
    letters = await wait_for(lambda: dead_letters(host.js, "waker-user"))
    assert letters[0]["data"]["deliveries"] == 1
    host.test_waker.error = WakeError("unavailable", "down", retryable=True)
    await pub(host.js, "test.wake.sent")
    await wait_for(lambda: _async(host.stats["waker-user"].retried >= 1))
    host.test_waker.error = None
    await wait_for(lambda: _async(host.stats["waker-user"].handled == 1))


async def test_watchers_publish_and_crashes_are_contained(host):
    msg = await wait_for(lambda: _get_last(host.js, "ev.test.tick.sent"))
    assert json.loads(msg.data)["id"] == "tick-1"
    await wait_for(lambda: _async(host.stats["crashy"].restarts >= 1))
    assert "watcher crashed" in host.test_logs.getvalue()
    assert host.health()["ok"] is True


async def test_logs_are_json_lines(host):
    await pub(host.js, "test.ok.sent", {"secret_looking": "do-not-log-me"})
    await wait_for(lambda: _async(host.stats["ok"].handled == 1))
    lines = [json.loads(line) for line in host.test_logs.getvalue().splitlines()]
    assert any(r["msg"] == "handled" and r["component"] == "ok" for r in lines)
    assert "do-not-log-me" not in host.test_logs.getvalue()


def test_delay_for():
    spec = kit.HandlerSpec("x", ["ev.x"], None, None, backoff=(5, 30, 120))
    assert [spec.delay_for(n) for n in (1, 2, 3, 4, 9)] == [5, 30, 120, 120, 120]


def test_stable_id():
    assert kit.stable_id("a", 1) == kit.stable_id("a", 1) != kit.stable_id("a", 2)


async def _async(value):
    return value


async def _last_id(js, seq):
    msg = await js.get_msg(bus.STREAM, seq)
    return json.loads(msg.data)["id"]


async def _get_last(js, subject):
    try:
        return await js.get_last_msg(bus.STREAM, subject)
    except Exception:
        return None
