"""Publish one event to the Tower bus.

    uv run python examples/publish.py
"""

import asyncio

from tower import bus, events


async def main() -> None:
    nc = await bus.connect()  # TOWER_NATS_URL, TOWER_NATS_TOKEN_FILE
    try:
        js = nc.jetstream()
        event = events.build(
            "example.greeting.sent",
            {"text": "hello from examples/publish.py"},
            source="tower/examples",
        )
        ack = await bus.publish(js, event, qualifiers=["demo"])  # subject ev.example.greeting.sent.demo
        print(f"published id={event['id']} stream={ack.stream} seq={ack.seq} duplicate={bool(ack.duplicate)}")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
