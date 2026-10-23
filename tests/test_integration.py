"""Against the live bus. Skipped unless TOWER_INTEGRATION=1."""

import os
import uuid

import pytest
from nats.js.api import AckPolicy, ConsumerConfig

from tower import bus, events

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("TOWER_INTEGRATION") != "1", reason="set TOWER_INTEGRATION=1 to use the live bus"),
]


async def test_publish_durable_consume_and_dedup():
    nc = await bus.connect()
    js = nc.jetstream()
    durable = f"test-{uuid.uuid4().hex[:8]}"
    run = uuid.uuid4().hex[:8]
    try:
        await bus.ensure_streams(js)
        ev = events.build("tower.test.published", {"run": run}, source="tower/tests")
        first = await bus.publish(js, ev, [run])
        again = await bus.publish(js, ev, [run])
        assert not first.duplicate
        assert again.duplicate and again.seq == first.seq

        sub = await js.pull_subscribe(
            f"ev.tower.test.published.{run}",
            durable=durable,
            stream=bus.STREAM,
            config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT),
        )
        msgs = await sub.fetch(10, timeout=2)
        assert len(msgs) == 1
        got = events.decode(msgs[0].data)
        assert got == ev
        assert msgs[0].headers["Nats-Msg-Id"] == ev["id"]
        await msgs[0].ack()

        letter = await bus.publish_dead_letter(js, "tests", ev, "boom")
        assert letter.stream == bus.DLQ_STREAM
    finally:
        try:
            await js.delete_consumer(bus.STREAM, durable)
        finally:
            await nc.close()


async def test_connection_without_token_refused(tmp_path):
    with pytest.raises(Exception):
        await bus.connect(token_file=str(tmp_path / "none"), max_reconnect_attempts=0, allow_reconnect=False)
