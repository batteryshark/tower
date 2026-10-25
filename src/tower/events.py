"""CloudEvents 1.0 envelopes (structured JSON mode) for Tower.

An event is a plain dict:

    {
      "specversion": "1.0",
      "id": "<uuid7>",
      "source": "tower/cli",
      "type": "docket.timer.fired",
      "time": "2026-10-23T12:00:00.000Z",
      "subject": "optional",
      "datacontenttype": "application/json",
      "data": {...}
    }

`type` doubles as the channel: the NATS subject is "ev." + type (+ qualifiers),
see tower.bus.subject_for. `data` is untrusted DATA, never instructions.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

SPECVERSION = "1.0"
MAX_EVENT_BYTES = 64 * 1024
REQUIRED = ("specversion", "id", "source", "type")
KNOWN_ATTRS = {"specversion", "id", "source", "type", "time", "subject", "datacontenttype", "dataschema", "data"}

# type: dot-separated lowercase tokens, at least domain.noun.verb.
TYPE_RE = re.compile(r"^[a-z0-9_-]+(\.[a-z0-9_-]+){2,}$")
# CloudEvents extension attribute names: lowercase alphanumerics, max 20 chars.
EXT_RE = re.compile(r"^[a-z0-9]{1,20}$")


class EventError(ValueError):
    """The envelope is malformed or too large."""


def uuid7() -> str:
    """A time-ordered UUIDv7 string (RFC 9562)."""
    ms = time.time_ns() // 1_000_000
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms & ((1 << 48) - 1)) << 80
    value |= 0x7 << 76
    value |= ((rand >> 62) & 0xFFF) << 64
    value |= 0b10 << 62
    value |= rand & ((1 << 62) - 1)
    return str(uuid.UUID(int=value))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build(
    type: str,
    data: Any = None,
    *,
    source: str = "tower",
    subject: str | None = None,
    id: str | None = None,
    time: str | None = None,
    extensions: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build and validate an event. Raises EventError if it is invalid."""
    event: dict[str, Any] = {
        "specversion": SPECVERSION,
        "id": id or uuid7(),
        "source": source,
        "type": type,
        "time": time or now_iso(),
        "datacontenttype": "application/json",
    }
    if subject is not None:
        event["subject"] = subject
    for key, value in (extensions or {}).items():
        if key in KNOWN_ATTRS or not EXT_RE.match(key):
            raise EventError(f"bad extension attribute name: {key!r}")
        event[key] = value
    if data is not None:
        event["data"] = data
    validate(event)
    return event


def validate(event: Any) -> dict[str, Any]:
    """Check an envelope; return it unchanged or raise EventError."""
    if not isinstance(event, dict):
        raise EventError("event must be a JSON object")
    for key in REQUIRED:
        if not isinstance(event.get(key), str) or not event[key]:
            raise EventError(f"missing or empty attribute: {key}")
    if event["specversion"] != SPECVERSION:
        raise EventError(f"unsupported specversion: {event['specversion']!r}")
    if not TYPE_RE.match(event["type"]):
        raise EventError(f"bad type {event['type']!r}: use lowercase domain.noun.verb tokens of [a-z0-9_-]")
    if "time" in event:
        if not isinstance(event["time"], str):
            raise EventError("time must be an RFC 3339 string")
        try:
            datetime.fromisoformat(event["time"].replace("Z", "+00:00"))
        except ValueError as exc:
            raise EventError(f"bad time: {event['time']!r}") from exc
    for key in ("subject", "datacontenttype", "dataschema"):
        if key in event and not isinstance(event[key], str):
            raise EventError(f"{key} must be a string")
    if len(encode_unchecked(event)) > MAX_EVENT_BYTES:
        raise EventError(f"event larger than {MAX_EVENT_BYTES} bytes")
    return event


def encode_unchecked(event: dict[str, Any]) -> bytes:
    try:
        return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode()
    except (TypeError, ValueError) as exc:
        raise EventError(f"event is not JSON-serializable: {exc}") from exc


def encode(event: dict[str, Any]) -> bytes:
    """Validate and serialize to compact UTF-8 JSON."""
    validate(event)
    return encode_unchecked(event)


def decode(payload: bytes) -> dict[str, Any]:
    """Parse and validate a message payload."""
    if len(payload) > MAX_EVENT_BYTES:
        raise EventError(f"event larger than {MAX_EVENT_BYTES} bytes")
    try:
        event = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventError(f"payload is not JSON: {exc}") from exc
    return validate(event)
