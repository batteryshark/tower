"""Handler kit: what a handler or watcher file sees.

A HANDLER is one file with a subscription and a handle function:

    NAME = "nod-answer-wake"
    SUBSCRIBE = "ev.nod.answer.received"        # or a list of filters
    MAX_DELIVER = 5                              # optional
    BACKOFF = [5, 30, 120, 600]                  # optional, seconds between attempts

    async def handle(event, ctx):
        ...

Return normally to ack. Raise kit.Drop(reason) to ack without acting (logged), kit.Fail(reason) to dead-letter
now, anything else (or kit.Retry) to retry with backoff until MAX_DELIVER, then dead-letter to dlq.<NAME>.
A WakeError from the waker retries when it is retryable and dead-letters otherwise.

A WATCHER is one file with a long-running loop that watches the outside world and publishes events:

    NAME = "nod-watcher"

    async def run(ctx):
        while True:
            ...
            await ctx.publish("nod.answer.received", data, id=stable_id)
            await asyncio.sleep(30)

The host restarts a watcher that crashes, with backoff.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable

from nats.errors import TimeoutError as NatsTimeout
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from . import bus, events
from . import waker as _waker

DEFAULT_MAX_DELIVER = 5
DEFAULT_BACKOFF = (5, 30, 120, 600)
DEFAULT_ACK_WAIT = 300
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
STATE_ROOT = Path("~/.tower/state")


class Retry(Exception):
    """Try again later (counts toward MAX_DELIVER)."""


class Drop(Exception):
    """Ack the event without acting; the reason is logged."""


class Fail(Exception):
    """Dead-letter the event now; retrying will not help."""


# -- logging ---------------------------------------------------------------


class Log:
    """Structured JSON-lines log to stdout (Fleet captures it). Never pass secrets or event data."""

    def __init__(self, component: str, stream=None):
        self.component = component
        self._stream = stream

    def _emit(self, level: str, msg: str, **fields: Any) -> None:
        record = {"ts": events.now_iso(), "level": level, "component": self.component, "msg": msg, **fields}
        out = self._stream or sys.stdout
        print(json.dumps(record, default=str, ensure_ascii=False), file=out, flush=True)

    def info(self, msg: str, **fields: Any) -> None:
        self._emit("info", msg, **fields)

    def warning(self, msg: str, **fields: Any) -> None:
        self._emit("warning", msg, **fields)

    def error(self, msg: str, **fields: Any) -> None:
        self._emit("error", msg, **fields)

    def child(self, component: str) -> "Log":
        return Log(component, self._stream)


# -- the waker, async ------------------------------------------------------


class AsyncWaker:
    """tower.waker with the two calls made awaitable (they block on HTTP)."""

    message_id_for = staticmethod(_waker.message_id_for)
    data_block = staticmethod(_waker.data_block)
    WakeError = _waker.WakeError

    def __init__(self, waker: Any = None):
        self._waker = waker or _waker

    async def message_session(self, session_id: str, text: str, **kwargs: Any) -> _waker.Admission:
        return await asyncio.to_thread(self._waker.message_session, session_id, text, **kwargs)

    async def start_session(self, agent: str, text: str, **kwargs: Any) -> _waker.Admission:
        return await asyncio.to_thread(self._waker.start_session, agent, text, **kwargs)


# -- context ---------------------------------------------------------------


@dataclass
class Context:
    name: str
    js: JetStreamContext
    log: Log
    config: dict[str, Any] = field(default_factory=dict)
    waker: AsyncWaker = field(default_factory=AsyncWaker)
    state_root: Path = STATE_ROOT
    stream: str = bus.STREAM

    @property
    def state_dir(self) -> Path:
        path = Path(os.path.expanduser(str(self.state_root))) / self.name
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        return path

    async def publish(
        self,
        type: str,
        data: Any = None,
        qualifiers: Iterable[str] = (),
        id: str | None = None,
        *,
        subject: str | None = None,
        source: str | None = None,
    ):
        """Publish an event; pass a stable id to make retries and restarts safe."""
        event = events.build(type, data, source=source or f"tower/{self.name}", subject=subject, id=id)
        ack = await bus.publish(self.js, event, qualifiers)
        self.log.info("published", type=type, id=event["id"], seq=ack.seq, duplicate=bool(ack.duplicate))
        return ack

    async def subscribe(
        self, filters: str | list[str], *, durable: str | None = None, batch: int = 10
    ) -> AsyncIterator[tuple[dict[str, Any] | None, Any]]:
        """For watchers: a durable pull consumer. Yields (event or None if invalid, msg); you ack/term msg."""
        durable = durable or self.name
        await ensure_consumer(self.js, self.stream, durable, filters, DEFAULT_MAX_DELIVER + 2, DEFAULT_ACK_WAIT)
        sub = await self.js.pull_subscribe_bind(durable, stream=self.stream)
        while True:
            try:
                msgs = await sub.fetch(batch, timeout=5)
            except (NatsTimeout, asyncio.TimeoutError):
                continue
            for msg in msgs:
                try:
                    event = events.decode(msg.data)
                except events.EventError:
                    event = None
                yield event, msg


async def ensure_consumer(
    js: JetStreamContext, stream: str, durable: str, filters: str | list[str], max_deliver: int, ack_wait: float
) -> None:
    """Create or update a durable explicit-ack pull consumer."""
    filters = [filters] if isinstance(filters, str) else list(filters)
    config = ConsumerConfig(
        durable_name=durable,
        ack_policy=AckPolicy.EXPLICIT,
        deliver_policy=DeliverPolicy.NEW,
        max_deliver=max_deliver,
        ack_wait=ack_wait,
        max_ack_pending=100,
    )
    if len(filters) == 1:
        config.filter_subject = filters[0]
    else:
        config.filter_subjects = filters
    await js.add_consumer(stream, config)


# -- loading files ---------------------------------------------------------


@dataclass
class HandlerSpec:
    name: str
    subscribe: list[str]
    handle: Callable[[dict[str, Any], Context], Awaitable[Any]]
    path: Path
    max_deliver: int = DEFAULT_MAX_DELIVER
    backoff: tuple[float, ...] = DEFAULT_BACKOFF
    ack_wait: float = DEFAULT_ACK_WAIT

    kind = "handler"

    def delay_for(self, delivered: int) -> float:
        """Backoff before the next attempt after `delivered` attempts."""
        if not self.backoff:
            return 0
        return float(self.backoff[min(delivered, len(self.backoff)) - 1])


@dataclass
class WatcherSpec:
    name: str
    run: Callable[[Context], Awaitable[Any]]
    path: Path

    kind = "watcher"


class LoadError(Exception):
    pass


def load_file(path: Path) -> HandlerSpec | WatcherSpec:
    """Import one handler or watcher file. Raises LoadError with a short reason."""
    module_name = f"tower_ext_{abs(hash(str(path.resolve())))}_{path.stem}"
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise LoadError("not importable")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except LoadError:
        raise
    except BaseException as exc:  # noqa: BLE001 - a broken file must not take the host down
        raise LoadError(f"import failed: {exc.__class__.__name__}: {exc}") from None

    name = getattr(module, "NAME", None)
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise LoadError("NAME must be lowercase [a-z0-9_-], up to 64 characters")
    handle, run = getattr(module, "handle", None), getattr(module, "run", None)
    if handle is not None and run is not None:
        raise LoadError("define handle (handler) or run (watcher), not both")
    if run is not None:
        if not asyncio.iscoroutinefunction(run):
            raise LoadError("run must be async def run(ctx)")
        return WatcherSpec(name, run, path)
    if handle is None or not asyncio.iscoroutinefunction(handle):
        raise LoadError("define async def handle(event, ctx) or async def run(ctx)")
    subscribe = getattr(module, "SUBSCRIBE", None)
    subscribe = [subscribe] if isinstance(subscribe, str) else subscribe
    if not subscribe or not all(isinstance(s, str) and s.startswith(f"{bus.EVENT_PREFIX}.") for s in subscribe):
        raise LoadError("SUBSCRIBE must be an ev.* filter or a list of them")
    max_deliver = getattr(module, "MAX_DELIVER", DEFAULT_MAX_DELIVER)
    backoff = tuple(getattr(module, "BACKOFF", DEFAULT_BACKOFF))
    ack_wait = getattr(module, "ACK_WAIT", DEFAULT_ACK_WAIT)
    if not isinstance(max_deliver, int) or max_deliver < 1:
        raise LoadError("MAX_DELIVER must be a positive int")
    if not all(isinstance(b, (int, float)) and b >= 0 for b in backoff):
        raise LoadError("BACKOFF must be a list of seconds")
    return HandlerSpec(name, list(subscribe), handle, path, max_deliver, backoff, float(ack_wait))


def discover(dirs: Iterable[Path]) -> tuple[list[HandlerSpec | WatcherSpec], list[dict[str, str]]]:
    """Load every *.py (not starting with '_') in dirs. Returns (specs, errors); later dirs cannot reuse a NAME."""
    specs: list[HandlerSpec | WatcherSpec] = []
    errors: list[dict[str, str]] = []
    seen: dict[str, Path] = {}
    for directory in dirs:
        directory = Path(os.path.expanduser(str(directory)))
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.py")):
            if path.name.startswith("_"):
                continue
            try:
                spec = load_file(path)
            except LoadError as exc:
                errors.append({"path": str(path), "error": str(exc)})
                continue
            if spec.name in seen:
                errors.append({"path": str(path), "error": f"NAME {spec.name!r} already used by {seen[spec.name]}"})
                continue
            seen[spec.name] = path
            specs.append(spec)
    return specs, errors


# -- config ----------------------------------------------------------------


def load_config(path: str | None = None) -> dict[str, Any]:
    """~/.tower/config.toml (or config.json, or TOWER_CONFIG). Tables are keyed by handler/watcher NAME."""
    candidates = [path or os.environ.get("TOWER_CONFIG") or "", "~/.tower/config.toml", "~/.tower/config.json"]
    for candidate in candidates:
        if not candidate:
            continue
        p = Path(os.path.expanduser(candidate))
        if not p.is_file():
            continue
        if p.suffix == ".toml":
            import tomllib

            return tomllib.loads(p.read_text())
        return json.loads(p.read_text())
    return {}


def stable_id(*parts: Any) -> str:
    """A deterministic event id from parts (for watchers that must not double-publish)."""
    import hashlib

    return hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).hexdigest()[:32]
