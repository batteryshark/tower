"""Connecting to the Tower bus (NATS JetStream), its streams, and publishing.

Environment:
    TOWER_NATS_URL         default nats://127.0.0.1:4222
    TOWER_NATS_TOKEN_FILE  default ~/.tower/secrets/nats.token (0600; read, never logged)
    TOWER_NATS_USER        optional: connect as this NATS user instead of with the token
    TOWER_NATS_PASSWORD_FILE  the user's password file (0600; read, never logged); required with TOWER_NATS_USER
    TOWER_NATS_CLIENT_CONFIG  the client pointer, default ~/.tower/nats/client.json (0600, no secrets):
                           {"mode": "token"} or {"mode": "user", "user": "tower", "password_file": "..."}

Auth precedence: explicit connect() arguments (user/password_file, or a token_file) > TOWER_NATS_USER +
TOWER_NATS_PASSWORD_FILE > client.json > the token (the default; a missing client.json means the token).

Streams:
    TOWER      ev.>   file storage, limits retention, 30 days, 5 GiB cap, 10 minute duplicate window
    TOWER_DLQ  dlq.>  file storage, 30 days

Channels are subjects: "ev." + event type + optional qualifier tokens, for example
ev.github.pr.opened.batteryshark.tower. Dead letters go to dlq.<handler>.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import nats
from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from . import events

DEFAULT_URL = "nats://127.0.0.1:4222"
DEFAULT_TOKEN_FILE = "~/.tower/secrets/nats.token"
DEFAULT_CLIENT_CONFIG = "~/.tower/nats/client.json"

STREAM = "TOWER"
DLQ_STREAM = "TOWER_DLQ"
EVENT_PREFIX = "ev"
DLQ_PREFIX = "dlq"
DAY = 24 * 60 * 60

STREAM_CONFIGS = (
    StreamConfig(
        name=STREAM,
        description="Tower events",
        subjects=[f"{EVENT_PREFIX}.>"],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.OLD,
        max_age=30 * DAY,
        max_bytes=5 * 1024**3,
        max_msg_size=events.MAX_EVENT_BYTES,
        duplicate_window=10 * 60,
    ),
    StreamConfig(
        name=DLQ_STREAM,
        description="Tower dead letters",
        subjects=[f"{DLQ_PREFIX}.>"],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.OLD,
        max_age=30 * DAY,
        max_bytes=1024**3,
        duplicate_window=10 * 60,
    ),
)

_TOKEN_RE = re.compile(r"[^a-z0-9_-]+")


def sanitize_token(value: str) -> str:
    """One subject token: lowercase [a-z0-9_-]; other runs become '-'. Raises ValueError if empty."""
    token = _TOKEN_RE.sub("-", str(value).strip().lower()).strip("-")
    if not token:
        raise ValueError(f"qualifier {value!r} is empty after sanitizing")
    return token


def subject_for(event_type: str, qualifiers: Iterable[str] = ()) -> str:
    """The subject for an event type: ev.<type>[.<qualifier>...]."""
    if not events.TYPE_RE.match(event_type):
        raise events.EventError(f"bad type {event_type!r}")
    return ".".join([EVENT_PREFIX, event_type, *(sanitize_token(q) for q in qualifiers)])


def dlq_subject(handler: str) -> str:
    return f"{DLQ_PREFIX}.{sanitize_token(handler)}"


def read_token(path: str | None = None) -> str | None:
    """The bus token from TOWER_NATS_TOKEN_FILE (or path); None if the file is missing."""
    token_path = Path(os.path.expanduser(path or os.environ.get("TOWER_NATS_TOKEN_FILE") or DEFAULT_TOKEN_FILE))
    try:
        return token_path.read_text().strip() or None
    except FileNotFoundError:
        return None


async def _log_error(exc: Exception) -> None:
    # One line instead of nats-py's default traceback; connect() still raises on failure.
    print(f"tower: {exc}", file=sys.stderr)


def _read_password(path: str) -> str:
    try:
        password = Path(os.path.expanduser(path)).read_text().strip()
    except OSError as exc:
        raise ValueError(f"NATS password file unreadable ({exc.__class__.__name__})") from None
    if not password:
        raise ValueError("NATS password file is empty")
    return password


def read_client_config(path: str | None = None) -> dict[str, Any] | None:
    """The client pointer (TOWER_NATS_CLIENT_CONFIG or ~/.tower/nats/client.json), or None if absent.

    Raises ValueError with a static reason when it exists but is not usable.
    """
    pointer = Path(os.path.expanduser(path or os.environ.get("TOWER_NATS_CLIENT_CONFIG") or DEFAULT_CLIENT_CONFIG))
    try:
        raw = pointer.read_text()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ValueError(f"NATS client config unreadable ({exc.__class__.__name__})") from None
    try:
        config = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("NATS client config is not JSON") from None
    if not isinstance(config, dict) or config.get("mode") not in ("token", "user"):
        raise ValueError('NATS client config needs "mode": "token" or "user"')
    if config["mode"] == "user":
        if not isinstance(config.get("user"), str) or not config["user"]:
            raise ValueError('NATS client config mode "user" needs "user"')
        if not isinstance(config.get("password_file"), str) or not config["password_file"]:
            raise ValueError('NATS client config mode "user" needs "password_file"')
    return config


def read_user_password(user: str | None = None, password_file: str | None = None) -> tuple[str, str] | None:
    """(user, password) when TOWER_NATS_USER (or user) is set, else None. Raises ValueError if the password is missing."""
    user = user or os.environ.get("TOWER_NATS_USER")
    if not user:
        return None
    path = password_file or os.environ.get("TOWER_NATS_PASSWORD_FILE")
    if not path:
        raise ValueError("TOWER_NATS_USER is set but TOWER_NATS_PASSWORD_FILE is not")
    return user, _read_password(path)


def auth_options(token_file: str | None = None, *, user: str | None = None, password_file: str | None = None,
                 client_config: str | None = None) -> dict[str, str]:
    """The nats.connect auth options, by the precedence above. Values are secrets: never log the result."""
    credentials = read_user_password(user, password_file)
    if credentials is None and token_file is None:
        config = read_client_config(client_config)
        if config is not None and config["mode"] == "user":
            credentials = config["user"], _read_password(config["password_file"])
    if credentials:
        return {"user": credentials[0], "password": credentials[1]}
    token = read_token(token_file)
    return {"token": token} if token else {}


def auth_mode(token_file: str | None = None) -> str:
    """'user <name>' or 'token' (no secrets), for status lines."""
    user = os.environ.get("TOWER_NATS_USER")
    if user:
        return f"user {user} (env)"
    config = None if token_file is not None else read_client_config()
    if config is not None and config["mode"] == "user":
        return f"user {config['user']} (client.json)"
    return "token"


async def connect(
    url: str | None = None,
    token_file: str | None = None,
    *,
    user: str | None = None,
    password_file: str | None = None,
    client_config: str | None = None,
    **options: Any,
) -> Client:
    """Connect to the bus, choosing auth by the precedence in this module's docstring. With a user the token is not
    sent (NATS refuses a token on a server configured with users). Extra options go to nats.connect."""
    servers = url or os.environ.get("TOWER_NATS_URL") or DEFAULT_URL
    auth = auth_options(token_file, user=user, password_file=password_file, client_config=client_config)
    for key, value in auth.items():
        options.setdefault(key, value)
    options.setdefault("name", "tower")
    options.setdefault("error_cb", _log_error)
    options.setdefault("connect_timeout", 5)
    options.setdefault("max_reconnect_attempts", 3)
    return await nats.connect(servers, **options)


async def ensure_streams(js: JetStreamContext) -> None:
    """Create the TOWER and TOWER_DLQ streams, or update them to the configured limits."""
    for config in STREAM_CONFIGS:
        try:
            await js.stream_info(config.name)
        except NotFoundError:
            await js.add_stream(config)
        else:
            await js.update_stream(config)


async def publish(
    js: JetStreamContext,
    event: dict[str, Any],
    qualifiers: Iterable[str] = (),
    *,
    subject: str | None = None,
):
    """Publish a validated event to JetStream; returns the PubAck (duplicate=True if dropped as a repeat).

    Nats-Msg-Id is the event id, so republishing the same event within 10 minutes is a no-op.
    """
    payload = events.encode(event)
    target = subject or subject_for(event["type"], qualifiers)
    return await js.publish(target, payload, headers={"Nats-Msg-Id": event["id"]})


async def publish_dead_letter(
    js: JetStreamContext, handler: str, event: dict[str, Any], error: str, *, id: str | None = None, **extra: Any
):
    """Publish a failed event to dlq.<handler>, wrapped as a tower.dlq.recorded event.

    Pass a stable id (e.g. from the stream sequence) so a retried dead-letter is deduplicated.
    If the wrapped event would exceed the size bound, only its id, type and source are kept.
    """
    def letter(original: Any) -> dict[str, Any]:
        return events.build(
            "tower.dlq.recorded",
            {"handler": handler, "error": error[:2000], "event": original, **extra},
            source=f"tower/handler/{handler}",
            subject=str(event.get("id", "")) or None,
            id=id,
        )

    try:
        wrapped = letter(event)
    except events.EventError:
        wrapped = letter({k: event.get(k) for k in ("id", "type", "source")} | {"truncated": True})
    return await publish(js, wrapped, subject=dlq_subject(handler))
