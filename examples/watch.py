"""Replay the last hour of events and keep tailing (what `tower watch --since 1h` does).

    uv run python examples/watch.py ['ev.github.>']
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone

from nats.js.api import ConsumerConfig, DeliverPolicy

from tower import bus, events


async def main(filter_subject: str) -> None:
    nc = await bus.connect()
    js = nc.jetstream()
    start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat().replace("+00:00", "Z")

    async def show(msg):
        try:
            event = events.decode(msg.data)
            print(f"{event['time']} {msg.subject} {event.get('data')}", flush=True)
        except events.EventError as exc:
            print(f"{msg.subject} [invalid: {exc}]", flush=True)

    # An ordered consumer is ephemeral: nothing is left behind on the server when we exit.
    await js.subscribe(
        filter_subject,
        stream=bus.STREAM,
        ordered_consumer=True,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_TIME, opt_start_time=start),
        cb=show,
    )
    try:
        await asyncio.Event().wait()
    finally:
        await nc.close()


if __name__ == "__main__":
    try:
        asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "ev.>"))
    except KeyboardInterrupt:
        pass
