"""Live viewer: page, SSE filter, bounded replay, resume by Last-Event-ID, dead letters, refusals."""

import asyncio
import json
from datetime import timedelta

import pytest
from aiohttp.test_utils import TestClient, TestServer

from tower import bus, events, kit, viewer
from tower.host import Host


@pytest.fixture
async def client(temp_bus, tmp_path):
    url, token_file = temp_bus
    (tmp_path / "handlers").mkdir()
    host = Host([tmp_path / "handlers"], url=url, token_file=token_file, config={}, port=None,
                log=kit.Log("host", open(tmp_path / "log", "w")), state_root=tmp_path / "state")
    await host.start()
    test_client = TestClient(TestServer(host.app))
    await test_client.start_server()
    test_client.tower = host
    try:
        yield test_client
    finally:
        await test_client.close()
        await host.stop()


async def pub(js, type, data=None, qualifiers=()):
    return await bus.publish(js, events.build(type, data or {}, source="tests"), qualifiers)


async def read_events(resp, count, timeout=5.0):
    """Read `count` SSE events: list of (id, item)."""
    out, current = [], {}
    async def run():
        while len(out) < count:
            line = (await resp.content.readline()).decode()
            if not line:
                return
            line = line.rstrip("\n")
            if line == "":
                if "data" in current:
                    out.append((current.get("id"), json.loads(current["data"])))
                current.clear()
            elif not line.startswith(":") and ":" in line:
                key, _, value = line.partition(":")
                current[key] = value.lstrip(" ")
    await asyncio.wait_for(run(), timeout)
    return out


def test_parsers():
    assert viewer.parse_filter(None) == "ev.>"
    assert viewer.parse_filter("ev.github.*.opened.>") == "ev.github.*.opened.>"
    for bad in ("dlq.>", ">", "ev..x", "ev.>.x", "ev.a b", "ev.$x", "x.ev"):
        with pytest.raises(viewer.BadRequest):
            viewer.parse_filter(bad)
    assert viewer.parse_since(None) == viewer.DEFAULT_REPLAY
    assert viewer.parse_since("5") == 5
    assert viewer.parse_since("999999") == viewer.MAX_REPLAY
    assert viewer.parse_since("15m") == timedelta(minutes=15)
    with pytest.raises(viewer.BadRequest):
        viewer.parse_since("yesterday")
    assert viewer.parse_last_event_id("7") == (7, None)
    assert viewer.parse_last_event_id("7.3") == (7, 3)
    assert viewer.parse_last_event_id("junk") == (None, None)


async def test_page_and_status(client):
    resp = await client.get("/tower/")
    assert resp.status == 200 and "text/html" in resp.headers["Content-Type"]
    body = await resp.text()
    assert "EventSource(\"events?" in body and "innerHTML" not in body.split("<script>")[1].replace("never innerHTML", "")
    assert "connect-src 'self'" in resp.headers["Content-Security-Policy"]
    resp = await client.get("/tower", allow_redirects=False)
    assert resp.status == 302 and resp.headers["Location"] == "/tower/"
    status = await (await client.get("/tower/status")).json()
    assert status["nats"] == "connected" and status["dead_letters"] == 0
    assert (await client.get("/tower/health")).status == 200


async def test_filter_and_live(client):
    js = client.tower.js
    await pub(js, "demo.viewer.check", {"n": 1})
    await pub(js, "other.thing.done", {"n": 2})
    resp = await client.get("/tower/events", params={"filter": "ev.demo.>"})
    assert resp.status == 200 and resp.headers["Content-Type"].startswith("text/event-stream")
    first = await read_events(resp, 1)
    assert first[0][1]["subject"] == "ev.demo.viewer.check" and first[0][1]["event"]["data"] == {"n": 1}
    assert first[0][0] == str(first[0][1]["seq"])
    await pub(js, "other.thing.done", {"n": 3})
    await pub(js, "demo.viewer.check", {"n": 4})
    live = await read_events(resp, 1)
    assert live[0][1]["event"]["data"] == {"n": 4}
    resp.close()


async def test_bounded_replay(client):
    for n in range(5):
        await pub(client.tower.js, "demo.replay.tick", {"n": n})
    resp = await client.get("/tower/events", params={"since": "2"})
    got = await read_events(resp, 2)
    assert [item["event"]["data"]["n"] for _, item in got] == [3, 4]
    resp.close()


async def test_resume_by_last_event_id(client):
    acks = [await pub(client.tower.js, "demo.resume.tick", {"n": n}) for n in range(6)]
    resp = await client.get("/tower/events", params={"since": "2"},
                            headers={"Last-Event-ID": str(acks[1].seq)})
    got = await read_events(resp, 4)
    # Last-Event-ID wins over since: everything after seq of n=1, no gap, no repeat.
    assert [item["event"]["data"]["n"] for _, item in got] == [2, 3, 4, 5]
    assert [int(i) for i, _ in got] == [a.seq for a in acks[2:]]
    resp.close()


async def test_dead_letters_toggle(client):
    js = client.tower.js
    await pub(js, "demo.dlq.base")
    await bus.publish_dead_letter(js, "demo-handler", events.build("demo.dlq.base", {}, source="tests"), "boom")
    resp = await client.get("/tower/events", params={"dlq": "1"})
    got = await read_events(resp, 2)
    streams = {item["stream"] for _, item in got}
    assert streams == {"TOWER", "TOWER_DLQ"}
    last_id = got[-1][0]
    assert last_id == "1.1"
    resp.close()
    # Resume with the combined id: nothing old repeats, new ones on both streams arrive.
    resp = await client.get("/tower/events", params={"dlq": "1"}, headers={"Last-Event-ID": last_id})
    await pub(js, "demo.dlq.next")
    got = await read_events(resp, 1)
    assert got[0][1]["subject"] == "ev.demo.dlq.next" and got[0][0] == "2.1"
    resp.close()


async def test_refusals(client):
    resp = await client.get("/tower/events", params={"filter": "dlq.>"})
    assert resp.status == 400
    resp = await client.get("/tower/events", params={"since": "soon"})
    assert resp.status == 400
    for method in ("post", "put", "delete"):
        assert (await client.request(method, "/tower/events")).status == 405
    client.tower.viewer.streams = viewer.MAX_STREAMS
    try:
        assert (await client.get("/tower/events")).status == 503
    finally:
        client.tower.viewer.streams = 0
