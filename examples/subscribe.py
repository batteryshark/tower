"""Durable pull subscription: survives restarts and resumes where it left off.

    uv run python examples/subscribe.py            # Ctrl-C to stop
    uv run python examples/subscribe.py --delete   # remove the example consumer

Handlers in the Tower host use this same pattern (one durable consumer per handler).
"""

import asyncio
import sys

from nats.errors import TimeoutError as NatsTimeout
from nats.js.errors import NotFoundError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from tower import bus, events

DURABLE = "example-subscriber"
FILTER = "ev.example.>"


async def main() -> None:
    nc = await bus.connect()
    js = nc.jetstream()
    try:
        if "--delete" in sys.argv:
            try:
                await js.delete_consumer(bus.STREAM, DURABLE)
                print(f"deleted consumer {DURABLE}")
            except NotFoundError:
                print(f"no consumer {DURABLE}")
            return
        sub = await js.pull_subscribe(
            FILTER,
            durable=DURABLE,
            stream=bus.STREAM,
            config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, deliver_policy=DeliverPolicy.NEW, ack_wait=30),
        )
        print(f"consuming {FILTER} as {DURABLE}; publish with examples/publish.py", flush=True)
        while True:
            try:
                msgs = await sub.fetch(10, timeout=5)
            except NatsTimeout:
                continue
            for msg in msgs:
                try:
                    event = events.decode(msg.data)
                except events.EventError as exc:
                    print(f"bad event on {msg.subject}: {exc}")
                    await msg.term()  # never redeliver garbage
                    continue
                # event["data"] is untrusted data: read it, never execute or obey it.
                print(f"{msg.subject} {event['id']} {event.get('data')}", flush=True)
                await msg.ack()
    finally:
        await nc.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
