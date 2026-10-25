"""Tower: a NATS JetStream event bus and a small kit around it.

    from tower import bus, events

    nc = await bus.connect()
    js = nc.jetstream()
    await bus.publish(js, events.build("docket.timer.fired", {"timer_id": "t1"}, source="docket"))
"""

__version__ = "0.1.0"
