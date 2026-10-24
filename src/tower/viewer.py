"""The live viewer: a read-only page and SSE stream of bus events, served by the handler host.

    GET /tower/          one self-contained page (viewer.html); relative URLs, so it works at the loopback root and
                         behind `tailscale serve --set-path /tower`
    GET /tower/events    text/event-stream; ?filter=ev.> (NATS wildcard inside ev.), ?since=200 (last N stream
                         messages) or 15m/2h/1d (duration), ?dlq=1 (also dead letters)
    GET /tower/status    handler states and dead-letter count for the status strip
    GET /tower/health    the host's /health

Each stream is an ephemeral ordered JetStream consumer (no acks, nothing durable). The SSE id is the stream
sequence ("<seq>", or "<seq>.<dlq seq>" with dlq=1), so the browser's Last-Event-ID resumes without gaps.
Read-only: there is no publish route and the bus token never leaves the host. Event data is untrusted; the page
renders it as text only.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from aiohttp import web
from nats.js import api

from . import bus

PAGE = Path(__file__).parent / "viewer.html"
DEFAULT_FILTER = f"{bus.EVENT_PREFIX}.>"
DEFAULT_REPLAY = 200
MAX_REPLAY = 1000
MAX_DURATION = timedelta(days=30)
MAX_STREAMS = 20
HEARTBEAT = 15.0
QUEUE_MAX = 2000
RETRY_MS = 3000

FILTER_RE = re.compile(r"^ev(\.([A-Za-z0-9_-]+|\*))*(\.>)?$")
DURATION_RE = re.compile(r"^(\d+)([smhd])$")
UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


class BadRequest(ValueError):
    pass


def parse_filter(value: str | None) -> str:
    subject = (value or DEFAULT_FILTER).strip()
    if len(subject) > 200 or not FILTER_RE.match(subject):
        raise BadRequest("filter must be a subject inside ev., for example ev.> or ev.github.*.opened.>")
    return subject


def parse_since(value: str | None) -> int | timedelta:
    """Last N stream messages (int, capped) or a duration back from now."""
    text = (value or "").strip().lower()
    if not text:
        return DEFAULT_REPLAY
    if text.isdigit():
        return min(int(text), MAX_REPLAY)
    match = DURATION_RE.match(text)
    if not match:
        raise BadRequest("since must be a count (200) or a duration (30s, 15m, 2h, 1d)")
    return min(timedelta(**{UNITS[match.group(2)]: int(match.group(1))}), MAX_DURATION)


def parse_last_event_id(value: str | None) -> tuple[int | None, int | None]:
    """'<seq>' or '<seq>.<dlq seq>' -> last delivered sequences (None when absent or garbled)."""
    parts = (value or "").strip().split(".")
    out: list[int | None] = []
    for index in range(2):
        part = parts[index] if index < len(parts) else ""
        out.append(int(part) if part.isdigit() else None)
    return out[0], out[1]


class Viewer:
    def __init__(self, host):
        self.host = host
        self.streams = 0
        self._closing = asyncio.Event()

    def install(self, app: web.Application) -> None:
        from .projects import Projects
        Projects().install(app)
        app.router.add_get("/tower", self._redirect)
        app.router.add_get("/tower/", self._page)
        app.router.add_get("/tower/events", self._events)
        app.router.add_get("/tower/status", self._status)
        app.router.add_get("/tower/health", self.host._health)
        app.on_shutdown.append(self._shutdown)

    async def _shutdown(self, app) -> None:
        self._closing.set()

    async def _redirect(self, request: web.Request) -> web.Response:
        raise web.HTTPFound("/tower/")

    async def _page(self, request: web.Request) -> web.Response:
        return web.Response(
            body=PAGE.read_bytes(),
            content_type="text/html",
            charset="utf-8",
            headers={
                "Cache-Control": "no-cache",
                "Content-Security-Policy": "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                "connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'self'",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "no-referrer",
            },
        )

    async def _status(self, request: web.Request) -> web.Response:
        health = self.host.health()
        handlers = {
            name: {k: info.get(k) for k in ("kind", "state", "handled", "dropped", "retried", "dead_lettered",
                                            "restarts", "last_event_at", "last_error", "subscribe")}
            for name, info in health["handlers"].items()
        }
        dead_letters = None
        if self.host.js is not None and health["ok"]:
            try:
                dead_letters = (await self.host.js.stream_info(bus.DLQ_STREAM)).state.messages
            except Exception:  # noqa: BLE001 - the strip shows "?"
                pass
        return web.json_response({
            "nats": health["nats"],
            "started_at": health["started_at"],
            "handlers": handlers,
            "load_errors": len(health["load_errors"]),
            "dead_letters": dead_letters,
            "streams": self.streams,
        }, headers={"Cache-Control": "no-cache"})

    async def _events(self, request: web.Request) -> web.StreamResponse:
        try:
            subject = parse_filter(request.query.get("filter"))
            since = parse_since(request.query.get("since"))
        except BadRequest as exc:
            return web.json_response({"error": str(exc)}, status=400)
        with_dlq = request.query.get("dlq") in ("1", "true", "yes", "on")
        last_ev, last_dlq = parse_last_event_id(request.headers.get("Last-Event-ID"))
        if self.host.js is None or not (self.host.nc and self.host.nc.is_connected):
            return web.json_response({"error": "bus not connected"}, status=503, headers={"Retry-After": "3"})
        if self.streams >= MAX_STREAMS:
            return web.json_response({"error": "too many viewers"}, status=503, headers={"Retry-After": "10"})

        self.streams += 1
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        overflow = asyncio.Event()
        subs = []
        try:
            plan = [(self.host.stream, subject, last_ev)]
            if with_dlq:
                plan.append((bus.DLQ_STREAM, f"{bus.DLQ_PREFIX}.>", last_dlq))
            for stream, filter_subject, last in plan:
                subs.append(await self._subscribe(stream, filter_subject, last, since, queue, overflow))

            response = web.StreamResponse(headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            })
            await response.prepare(request)
            await response.write(f"retry: {RETRY_MS}\n: tower viewer {subject}\n\n".encode())
            cursor = {self.host.stream: last_ev or 0, bus.DLQ_STREAM: last_dlq or 0}
            while not self._closing.is_set() and not overflow.is_set():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT)
                except asyncio.TimeoutError:
                    await response.write(b": ping\n\n")
                    continue
                cursor[item["stream"]] = item["seq"]
                event_id = str(cursor[self.host.stream])
                if with_dlq:
                    event_id += f".{cursor[bus.DLQ_STREAM]}"
                payload = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
                await response.write(f"id: {event_id}\nevent: tower\ndata: {payload}\n\n".encode())
            # Overflow or shutdown: end the stream; the browser reconnects with Last-Event-ID, so nothing is lost.
            return response
        finally:
            self.streams -= 1
            for sub in subs:
                try:
                    await sub.unsubscribe()
                except Exception:  # noqa: BLE001 - the server drops the ephemeral consumer on inactivity anyway
                    pass

    async def _subscribe(self, stream, filter_subject, last, since, queue, overflow):
        config = api.ConsumerConfig(inactive_threshold=30)
        if last is not None:
            config.deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config.opt_start_seq = last + 1
        elif isinstance(since, timedelta):
            config.deliver_policy = api.DeliverPolicy.BY_START_TIME
            config.opt_start_time = (datetime.now(timezone.utc) - since).isoformat().replace("+00:00", "Z")
        else:
            state = (await self.host.js.stream_info(stream)).state
            config.deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config.opt_start_seq = max(1, state.first_seq, state.last_seq - since + 1)

        async def on_message(msg):
            if overflow.is_set():
                return
            try:
                queue.put_nowait(_item(stream, msg))
            except asyncio.QueueFull:
                overflow.set()

        return await self.host.js.subscribe(
            filter_subject, stream=stream, cb=on_message, ordered_consumer=True, config=config
        )


def _item(stream: str, msg) -> dict[str, Any]:
    meta = msg.metadata
    try:
        event = json.loads(msg.data)
    except ValueError:
        event = {"raw": msg.data[:4000].decode(errors="replace")}
    stamp = meta.timestamp
    return {
        "stream": stream,
        "seq": meta.sequence.stream,
        "subject": msg.subject,
        "received": stamp.isoformat() if isinstance(stamp, datetime) else stamp,
        "event": event,
    }
