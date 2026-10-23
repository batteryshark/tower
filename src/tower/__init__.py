"""Tower: a NATS JetStream event bus and a small kit around it.

    from tower import bus, events

    nc = await bus.connect()
    js = nc.jetstream()
    await bus.publish(js, events.build("nod.answer.received", {"request_id": "r1"}, source="nod"))
"""

__version__ = "0.1.0"
