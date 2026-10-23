import json
import uuid

import pytest

from tower import events


def test_build_fills_required_attributes():
    ev = events.build("nod.answer.received", {"request_id": "r1"}, source="nod", subject="r1")
    assert ev["specversion"] == "1.0"
    assert ev["type"] == "nod.answer.received"
    assert ev["source"] == "nod"
    assert ev["subject"] == "r1"
    assert ev["datacontenttype"] == "application/json"
    assert ev["time"].endswith("Z")
    assert uuid.UUID(ev["id"]).version == 7


def test_uuid7_is_time_ordered():
    ids = [events.uuid7() for _ in range(50)]
    assert [i[:13] for i in ids] == sorted(i[:13] for i in ids)
    assert len(set(ids)) == 50


def test_roundtrip():
    ev = events.build("docket.timer.fired", {"n": 1})
    assert events.decode(events.encode(ev)) == ev


@pytest.mark.parametrize("bad_type", ["", "nod", "nod.answer", "Nod.Answer.Received", "nod.answer.re ceived", "nod..x.y"])
def test_bad_types_rejected(bad_type):
    with pytest.raises(events.EventError):
        events.build(bad_type, {})


def test_validate_rejects_missing_and_wrong_specversion():
    ev = events.build("a.b.c")
    for key in events.REQUIRED:
        broken = dict(ev)
        del broken[key]
        with pytest.raises(events.EventError):
            events.validate(broken)
    with pytest.raises(events.EventError):
        events.validate(dict(ev, specversion="0.3"))
    with pytest.raises(events.EventError):
        events.validate(dict(ev, time="yesterday"))
    with pytest.raises(events.EventError):
        events.validate(["not", "an", "object"])


def test_size_bound():
    events.build("a.b.c", {"x": "y" * 60_000})
    with pytest.raises(events.EventError):
        events.build("a.b.c", {"x": "y" * events.MAX_EVENT_BYTES})
    with pytest.raises(events.EventError):
        events.decode(b"{" + b" " * events.MAX_EVENT_BYTES + b"}")


def test_decode_rejects_garbage():
    with pytest.raises(events.EventError):
        events.decode(b"not json")
    with pytest.raises(events.EventError):
        events.decode(json.dumps({"specversion": "1.0"}).encode())


def test_extensions():
    ev = events.build("a.b.c", extensions={"origin": "ses_1"})
    assert ev["origin"] == "ses_1"
    with pytest.raises(events.EventError):
        events.build("a.b.c", extensions={"Bad-Name": 1})
    with pytest.raises(events.EventError):
        events.build("a.b.c", extensions={"type": "x"})


def test_unserializable_data():
    with pytest.raises(events.EventError):
        events.build("a.b.c", {"x": object()})
