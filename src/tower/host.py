"""The handler host: runs every handler and watcher file, one durable consumer per handler.

    tower host [--dir DIR ...] [--no-builtin] [--port 8796]

Handler files come from the built-in dir (src/tower/handlers) and ~/.tower/handlers (or TOWER_HANDLERS_DIR).
A file that fails to load is logged and skipped; a handler that raises only affects its own event.

Per event: ack on success or kit.Drop; nak with BACKOFF on failure; after MAX_DELIVER attempts (or kit.Fail, a
non-retryable WakeError, or an invalid envelope) publish a tower.dlq.recorded event to dlq.<handler> and term it.

HTTP on 127.0.0.1:8796: GET /health, and the live viewer under /tower/ (tower.viewer).
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aiohttp import web
from nats.errors import TimeoutError as NatsTimeout

from . import bus, events, kit
from .viewer import Viewer
from .waker import WakeError

BUILTIN_DIR = Path(__file__).parent / "handlers"
EXTRA_DIR = "~/.tower/handlers"
DEFAULT_PORT = 8796


def default_dirs(builtin: bool = True, extra: list[str] | None = None) -> list[Path]:
    dirs = [BUILTIN_DIR] if builtin else []
    dirs.append(Path(os.path.expanduser(os.environ.get("TOWER_HANDLERS_DIR") or EXTRA_DIR)))
    dirs.extend(Path(os.path.expanduser(d)) for d in extra or [])
    return dirs


@dataclass
class Stats:
    handled: int = 0
    dropped: int = 0
    retried: int = 0
    dead_lettered: int = 0
    last_error: str | None = None
    last_event_at: str | None = None
    state: str = "starting"
    restarts: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


class Host:
    def __init__(
        self,
        dirs: list[Path],
        *,
        url: str | None = None,
        token_file: str | None = None,
        config: dict[str, Any] | None = None,
        port: int | None = DEFAULT_PORT,
        log: kit.Log | None = None,
        state_root: Path = kit.STATE_ROOT,
        waker: kit.AsyncWaker | None = None,
        stream: str = bus.STREAM,
    ):
        self.dirs = dirs
        self.url, self.token_file = url, token_file
        self.config = config if config is not None else kit.load_config()
        self.port = port
        self.log = log or kit.Log("host")
        self.state_root = state_root
        self.waker = waker or kit.AsyncWaker()
        self.stream = stream
        self.specs: list[kit.HandlerSpec | kit.WatcherSpec] = []
        self.load_errors: list[dict[str, str]] = []
        self.stats: dict[str, Stats] = {}
        self.app = web.Application()
        self.app.router.add_get("/health", self._health)
        self.viewer = Viewer(self)
        self.viewer.install(self.app)
        self._tasks: list[asyncio.Task] = []
        self._runner: web.AppRunner | None = None
        self._stopping = asyncio.Event()
        self.nc = None
        self.js = None
        self.started_at = events.now_iso()

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        self.nc = await bus.connect(
            self.url, self.token_file, name="tower-host", max_reconnect_attempts=-1, reconnect_time_wait=2
        )
        self.js = self.nc.jetstream()
        await bus.ensure_streams(self.js)
        self.specs, self.load_errors = kit.discover(self.dirs)
        for error in self.load_errors:
            self.log.error("load failed", **error)
        for spec in self.specs:
            self.stats[spec.name] = Stats()
            ctx = self._context(spec.name)
            if isinstance(spec, kit.HandlerSpec):
                try:
                    # Server-side max_deliver leaves headroom so a crash on the last attempt still dead-letters.
                    await kit.ensure_consumer(
                        self.js, self.stream, spec.name, spec.subscribe, spec.max_deliver + 2, spec.ack_wait
                    )
                except Exception as exc:  # noqa: BLE001
                    self._fail_spec(spec, f"consumer setup failed: {exc}")
                    continue
                self._tasks.append(asyncio.create_task(self._guard(spec, self._run_handler(spec, ctx)), name=spec.name))
            else:
                self._tasks.append(asyncio.create_task(self._guard(spec, self._run_watcher(spec, ctx)), name=spec.name))
            self.log.info("loaded", kind=spec.kind, name=spec.name, path=str(spec.path),
                          subscribe=getattr(spec, "subscribe", None))
        if self.port is not None:
            self._runner = web.AppRunner(self.app, access_log=None)
            await self._runner.setup()
            await web.TCPSite(self._runner, "127.0.0.1", self.port).start()
        self.log.info("host started", handlers=sum(isinstance(s, kit.HandlerSpec) for s in self.specs),
                      watchers=sum(isinstance(s, kit.WatcherSpec) for s in self.specs),
                      load_errors=len(self.load_errors), port=self.port)

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._runner:
            await self._runner.cleanup()
        if self.nc and not self.nc.is_closed:
            await self.nc.drain()
        self.log.info("host stopped")

    async def run_forever(self) -> None:
        await self.start()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._stopping.set)
        try:
            await self._stopping.wait()
        finally:
            await self.stop()

    def _context(self, name: str) -> kit.Context:
        return kit.Context(
            name=name,
            js=self.js,
            log=self.log.child(name),
            config=dict(self.config.get(name) or {}),
            waker=self.waker,
            state_root=self.state_root,
            stream=self.stream,
        )

    async def _guard(self, spec, coro) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one handler never takes the host down
            self._fail_spec(spec, _describe(exc))

    def _fail_spec(self, spec, error: str) -> None:
        self.stats[spec.name].state = "failed"
        self.stats[spec.name].last_error = error[:300]
        self.log.error("handler disabled", name=spec.name, error=error[:300])

    # -- handlers ------------------------------------------------------------

    async def _run_handler(self, spec: kit.HandlerSpec, ctx: kit.Context) -> None:
        stats = self.stats[spec.name]
        sub = await self.js.pull_subscribe_bind(spec.name, stream=self.stream)
        stats.state = "running"
        while not self._stopping.is_set():
            try:
                msgs = await sub.fetch(10, timeout=5)
            except (NatsTimeout, asyncio.TimeoutError):
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - e.g. reconnecting
                ctx.log.warning("fetch failed", error=f"{exc.__class__.__name__}: {exc}"[:300])
                await asyncio.sleep(2)
                continue
            for msg in msgs:
                await self.process(spec, ctx, msg)

    async def process(self, spec: kit.HandlerSpec, ctx: kit.Context, msg) -> str:
        """Run one delivery; returns the outcome (handled, dropped, retry, dead_letter)."""
        stats = self.stats.setdefault(spec.name, Stats())
        meta = msg.metadata
        delivered = meta.num_delivered
        seq = meta.sequence.stream
        stats.last_event_at = events.now_iso()
        info = {"seq": seq, "subject": msg.subject, "delivered": delivered}
        try:
            event = events.decode(msg.data)
        except events.EventError as exc:
            raw = msg.data[:4000].decode(errors="replace")
            return await self._dead_letter(spec, ctx, msg, {"raw": raw}, f"invalid event: {exc}", info)
        info["id"] = event["id"]
        started = time.monotonic()
        try:
            await asyncio.wait_for(spec.handle(event, ctx), timeout=max(1.0, spec.ack_wait * 0.9))
        except asyncio.CancelledError:
            raise
        except kit.Drop as exc:
            await msg.ack()
            stats.dropped += 1
            ctx.log.info("dropped", reason=str(exc)[:300], **info)
            return "dropped"
        except (kit.Fail, WakeError) as exc:
            if isinstance(exc, WakeError) and exc.retryable:
                return await self._retry(spec, ctx, msg, event, exc, info)
            return await self._dead_letter(spec, ctx, msg, event, _describe(exc), info)
        except Exception as exc:  # noqa: BLE001 - Retry and anything else
            return await self._retry(spec, ctx, msg, event, exc, info)
        await msg.ack()
        stats.handled += 1
        ctx.log.info("handled", ms=int((time.monotonic() - started) * 1000), **info)
        return "handled"

    async def _retry(self, spec, ctx, msg, event, exc, info) -> str:
        stats = self.stats[spec.name]
        error = _describe(exc)
        stats.last_error = error[:300]
        if info["delivered"] >= spec.max_deliver:
            return await self._dead_letter(spec, ctx, msg, event, f"gave up after {info['delivered']} attempts: {error}", info)
        delay = spec.delay_for(info["delivered"])
        await msg.nak(delay=delay)
        stats.retried += 1
        ctx.log.warning("retry", error=error[:300], delay=delay, **info)
        return "retry"

    async def _dead_letter(self, spec, ctx, msg, event, error, info) -> str:
        stats = self.stats[spec.name]
        stats.last_error = error[:300]
        try:
            await bus.publish_dead_letter(
                self.js, spec.name, event, error,
                id=f"dlq-{spec.name}-{self.stream}-{info['seq']}",
                deliveries=info["delivered"], stream_seq=info["seq"], original_subject=info["subject"],
            )
        except Exception as dlq_exc:  # noqa: BLE001 - keep the event; try again later
            await msg.nak(delay=30)
            ctx.log.error("dead-letter publish failed", error=_describe(dlq_exc)[:300], **info)
            return "retry"
        await msg.term()
        stats.dead_lettered += 1
        ctx.log.error("dead-lettered", error=error[:300], dlq=bus.dlq_subject(spec.name), **info)
        return "dead_letter"

    # -- watchers -------------------------------------------------------------

    async def _run_watcher(self, spec: kit.WatcherSpec, ctx: kit.Context) -> None:
        stats = self.stats[spec.name]
        delay = 1.0
        while not self._stopping.is_set():
            started = time.monotonic()
            stats.state = "running"
            try:
                await spec.run(ctx)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                stats.last_error = _describe(exc)[:300]
                stats.restarts += 1
                stats.state = "restarting"
                ctx.log.error("watcher crashed", error=stats.last_error, retry_in=delay)
            else:
                stats.state = "finished"
                ctx.log.info("watcher finished")
                return
            if time.monotonic() - started > 60:
                delay = 1.0
            await asyncio.sleep(delay)
            delay = min(delay * 2, 300)

    # -- http -------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        connected = bool(self.nc and self.nc.is_connected)
        return {
            "ok": connected,
            "nats": "connected" if connected else "disconnected",
            "started_at": self.started_at,
            "handlers": {
                spec.name: {"kind": spec.kind, "path": str(spec.path),
                            "subscribe": getattr(spec, "subscribe", None), **vars(self.stats[spec.name])}
                for spec in self.specs
            },
            "load_errors": self.load_errors,
        }

    async def _health(self, request: web.Request) -> web.Response:
        body = self.health()
        return web.json_response(body, status=200 if body["ok"] else 503)


def _describe(exc: BaseException) -> str:
    if isinstance(exc, WakeError):
        return f"WakeError {exc.code}: {exc.detail}"
    text = str(exc)
    return f"{exc.__class__.__name__}: {text}" if text else exc.__class__.__name__
