"""The `tower` command: pub, sub, watch, streams."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from nats.js.api import ConsumerConfig, DeliverPolicy

from . import bus, events

DEFAULT_FILTER = "ev.>"
_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> timedelta:
    """'90s', '10m', '2h', '7d' -> timedelta."""
    match = _DURATION_RE.match(text.strip())
    if not match:
        raise argparse.ArgumentTypeError(f"bad duration {text!r}: use a number with s, m, h or d (e.g. 10m)")
    return timedelta(seconds=float(match.group(1)) * _UNITS[match.group(2)])


def stream_for(filter_subject: str) -> str:
    return bus.DLQ_STREAM if filter_subject.split(".", 1)[0] == bus.DLQ_PREFIX else bus.STREAM


def format_event(subject: str, payload: bytes, seq: int | None = None) -> str:
    """One readable line per message; malformed payloads are shown, not trusted."""
    prefix = f"#{seq} " if seq is not None else ""
    try:
        event = events.decode(payload)
    except events.EventError as exc:
        return f"{prefix}{subject}  [invalid: {exc}] {payload[:200]!r}"
    when = event.get("time", "-")
    data = json.dumps(event.get("data"), separators=(",", ":"), ensure_ascii=False)
    if len(data) > 400:
        data = data[:400] + "..."
    extra = f" subject={event['subject']}" if event.get("subject") else ""
    return f"{prefix}{when}  {subject}  source={event['source']}{extra}  id={event['id']}\n    {data}"


async def cmd_pub(args: argparse.Namespace) -> int:
    try:
        data: Any = json.loads(args.data) if args.data is not None else None
    except json.JSONDecodeError as exc:
        print(f"tower pub: --data is not JSON: {exc}", file=sys.stderr)
        return 2
    try:
        event = events.build(args.type, data, source=args.source, subject=args.subject, id=args.id)
        subject = bus.subject_for(args.type, args.qualifier)
    except (events.EventError, ValueError) as exc:
        print(f"tower pub: {exc}", file=sys.stderr)
        return 2
    nc = await bus.connect()
    try:
        js = nc.jetstream()
        ack = await bus.publish(js, event, args.qualifier)
    finally:
        await nc.close()
    note = " (duplicate, dropped)" if ack.duplicate else ""
    print(f"published {subject} id={event['id']} stream={ack.stream} seq={ack.seq}{note}")
    return 0


async def cmd_sub(args: argparse.Namespace) -> int:
    nc = await bus.connect()
    done = asyncio.Event()
    count = 0

    async def on_msg(msg):
        nonlocal count
        if done.is_set():
            return
        if args.raw:
            print(msg.subject, msg.data.decode(errors="replace"), flush=True)
        else:
            print(format_event(msg.subject, msg.data), flush=True)
        count += 1
        if args.count and count >= args.count:
            done.set()

    await nc.subscribe(args.filter, cb=on_msg)
    print(f"listening on {args.filter} (live only; Ctrl-C to stop)", file=sys.stderr, flush=True)
    try:
        await done.wait()
    finally:
        await nc.close()
    return 0


async def cmd_watch(args: argparse.Namespace) -> int:
    filter_subject = args.filter or DEFAULT_FILTER
    if args.all:
        config = ConsumerConfig(deliver_policy=DeliverPolicy.ALL)
        mode = "from the start of the stream"
    elif args.since:
        start = datetime.now(timezone.utc) - args.since
        config = ConsumerConfig(
            deliver_policy=DeliverPolicy.BY_START_TIME,
            opt_start_time=start.isoformat().replace("+00:00", "Z"),
        )
        mode = f"since {start.isoformat(timespec='seconds')}"
    else:
        config = ConsumerConfig(deliver_policy=DeliverPolicy.NEW)
        mode = "new events only"
    nc = await bus.connect()
    js = nc.jetstream()
    done = asyncio.Event()
    count = 0

    async def on_msg(msg):
        nonlocal count
        if done.is_set():
            return
        meta = msg.metadata
        print(format_event(msg.subject, msg.data, meta.sequence.stream), flush=True)
        count += 1
        if (args.count and count >= args.count) or (args.no_follow and meta.num_pending == 0):
            done.set()

    stream = stream_for(filter_subject)
    if args.no_follow:
        info = await js.stream_info(stream)
        if info.state.messages == 0:
            await nc.close()
            return 0
    await js.subscribe(filter_subject, stream=stream, ordered_consumer=True, config=config, cb=on_msg)
    print(f"watching {filter_subject} on {stream}, {mode}", file=sys.stderr, flush=True)
    try:
        if args.no_follow:
            # Stop when caught up, or after a quiet second if the filter matches nothing.
            while not done.is_set():
                seen = count
                try:
                    await asyncio.wait_for(done.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    if count == seen:
                        break
        else:
            await done.wait()
    finally:
        await nc.close()
    return 0


def _name(value: Any) -> str:
    return str(getattr(value, "value", value) or "-")


async def cmd_streams(args: argparse.Namespace) -> int:
    nc = await bus.connect()
    try:
        js = nc.jetstream()
        if args.ensure:
            await bus.ensure_streams(js)
        for name in (bus.STREAM, bus.DLQ_STREAM):
            try:
                info = await js.stream_info(name)
            except Exception as exc:  # noqa: BLE001 - show what is missing
                print(f"{name}: unavailable ({exc.__class__.__name__}); run `tower streams --ensure`")
                continue
            cfg, state = info.config, info.state
            max_age_days = (cfg.max_age or 0) / 86400
            print(
                f"{name}: subjects={','.join(cfg.subjects or [])} messages={state.messages} bytes={state.bytes} "
                f"first_seq={state.first_seq} last_seq={state.last_seq} consumers={state.consumer_count}\n"
                f"  storage={_name(cfg.storage)} retention={_name(cfg.retention)} "
                f"max_age={max_age_days:g}d max_bytes={cfg.max_bytes} dup_window={cfg.duplicate_window:g}s"
            )
            for consumer in await js.consumers_info(name):
                c = consumer.config
                print(
                    f"  consumer {consumer.name}: filter={c.filter_subject or '-'} pending={consumer.num_pending} "
                    f"ack_pending={consumer.num_ack_pending} redelivered={consumer.num_redelivered}"
                )
    finally:
        await nc.close()
    return 0


async def cmd_host(args: argparse.Namespace) -> int:
    from .host import Host, default_dirs

    port = None if args.port == 0 else args.port
    host = Host(default_dirs(builtin=not args.no_builtin, extra=args.dir), port=port)
    await host.run_forever()
    return 0


def _host_health(port: int) -> dict | None:
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as response:
            return json.load(response)
    except Exception:  # noqa: BLE001 - 503 or not running
        return None


async def cmd_handlers(args: argparse.Namespace) -> int:
    health = _host_health(args.port)
    if health is None:
        print(f"host: not reachable on 127.0.0.1:{args.port} (fleet status tower)")
    else:
        print(f"host: nats={health['nats']} started={health['started_at']}")
        for name, h in health["handlers"].items():
            subscribe = ",".join(h.get("subscribe") or []) or "-"
            print(
                f"  {h['kind']:<7} {name:<20} {h['state']:<10} {subscribe}  handled={h['handled']} "
                f"dropped={h['dropped']} retried={h['retried']} dlq={h['dead_lettered']} restarts={h['restarts']}"
            )
            if h.get("last_error"):
                print(f"          last error: {h['last_error']}")
        for error in health.get("load_errors") or []:
            print(f"  LOAD ERROR {error['path']}: {error['error']}")
    nc = await bus.connect()
    try:
        js = nc.jetstream()
        print("consumers (TOWER):")
        for consumer in await js.consumers_info(bus.STREAM):
            if not consumer.config.durable_name:
                continue
            c = consumer.config
            filters = c.filter_subject or ",".join(c.filter_subjects or []) or "-"
            print(
                f"  {consumer.name:<20} {filters}  lag={consumer.num_pending} ack_pending={consumer.num_ack_pending} "
                f"redelivered={consumer.num_redelivered}"
            )
        info = await js.stream_info(bus.DLQ_STREAM)
        print(f"dead letters: {info.state.messages} (tower dlq [handler])")
    finally:
        await nc.close()
    return 0


async def cmd_dlq(args: argparse.Namespace) -> int:
    filter_subject = bus.dlq_subject(args.handler) if args.handler else f"{bus.DLQ_PREFIX}.>"
    watch_args = argparse.Namespace(
        filter=filter_subject, all=not args.since, since=args.since, count=args.count, no_follow=not args.follow
    )
    return await cmd_watch(watch_args)


async def cmd_nod(args: argparse.Namespace) -> int:
    from . import nod

    if args.nod_command == "pending":
        for rid in nod.pending():
            print(rid)
        return 0
    if not 1 <= args.limit <= 500:
        raise ValueError("--limit must be 1..500")
    report = await asyncio.to_thread(nod.reconcile, args.after, args.limit)
    for row in report["missing"]:
        print(f"missing seq={row['seq']} id={row['id']} request_id={row['request_id']} revision={row['revision']}")
    print(f"scanned={report['scanned']} answers={report['answers']} missing={len(report['missing'])} "
          f"next_cursor={report['next_cursor']} (nothing republished)")
    if args.deliver_from_seq is None:
        return 0
    outcomes = await nod.deliver(report["missing"], report["envelopes"], args.deliver_from_seq, args.mode)
    for row in outcomes:
        print(f"catch-up ({args.mode}) seq={row['seq']} id={row['id']} {row['outcome']} {row.get('reason', '')}".rstrip())
    return 1 if any(row["outcome"] == "failed" for row in outcomes) else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tower", description="Tower event bus (NATS JetStream).")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("pub", help="publish an event")
    p.add_argument("type", help="event type, e.g. nod.answer.received (subject becomes ev.<type>)")
    p.add_argument("--data", help="event data as JSON")
    p.add_argument("--source", default="tower/cli", help="event source (default tower/cli)")
    p.add_argument("--subject", help="CloudEvents subject attribute (what the event is about)")
    p.add_argument("--id", help="event id (default: new uuid7); reuse it to test duplicate dropping")
    p.add_argument("--qualifier", "-q", action="append", default=[], help="extra subject token, repeatable")
    p.set_defaults(func=cmd_pub)

    p = sub.add_parser("sub", help="live, ephemeral subscription (no replay)")
    p.add_argument("filter", nargs="?", default=DEFAULT_FILTER, help="subject filter (default ev.>)")
    p.add_argument("--raw", action="store_true", help="print subject and raw JSON")
    p.add_argument("--count", type=int, default=0, help="exit after N messages")
    p.set_defaults(func=cmd_sub)

    p = sub.add_parser("watch", help="pretty tail of the stream, with replay")
    p.add_argument("filter", nargs="?", help="subject filter (default ev.>; dlq.> watches dead letters)")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--since", type=parse_duration, help="replay from this long ago, e.g. 10m, 2h, 7d")
    group.add_argument("--all", action="store_true", help="replay everything in the stream")
    p.add_argument("--count", type=int, default=0, help="exit after N messages")
    p.add_argument("--no-follow", action="store_true", help="exit once caught up")
    p.set_defaults(func=cmd_watch)

    p = sub.add_parser("streams", help="show stream and consumer info")
    p.add_argument("--ensure", action="store_true", help="create or update the TOWER and TOWER_DLQ streams first")
    p.set_defaults(func=cmd_streams)

    p = sub.add_parser("host", help="run the handler host (handlers and watchers)")
    p.add_argument("--dir", action="append", default=[], help="extra handler dir, repeatable")
    p.add_argument("--no-builtin", action="store_true", help="skip the built-in handlers")
    p.add_argument("--port", type=int, default=8796, help="health port on 127.0.0.1 (0 = none)")
    p.set_defaults(func=cmd_host)

    p = sub.add_parser("handlers", help="list handlers, their state and consumer lag")
    p.add_argument("--port", type=int, default=8796)
    p.set_defaults(func=cmd_handlers)

    p = sub.add_parser("dlq", help="show dead letters (all, or one handler)")
    p.add_argument("handler", nargs="?")
    p.add_argument("--since", type=parse_duration)
    p.add_argument("--count", type=int, default=0)
    p.add_argument("--follow", action="store_true", help="keep following after the backlog")
    p.set_defaults(func=cmd_dlq)

    p = sub.add_parser("nod", help="Nod uplift helpers (report only)")
    nod_sub = p.add_subparsers(dest="nod_command", required=True)
    q = nod_sub.add_parser("reconcile", help="answer.recorded events in Nod's journal missing from the ledger")
    q.add_argument("--after", type=int, required=True, help="Nod journal cursor (seq) to start after")
    q.add_argument("--limit", type=int, default=100, help="page size, 1..500 (default 100)")
    q.add_argument("--deliver-from-seq", type=int, metavar="N",
                   help="one-off catch-up: run missing events with seq >= N through the handler in this process")
    q.add_argument("--mode", choices=["shadow", "live"], default="shadow", help="catch-up mode (default shadow)")
    q.set_defaults(func=cmd_nod)
    q = nod_sub.add_parser("pending", help="open request ids in the old watcher ledger (ids only)")
    q.set_defaults(func=cmd_nod)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return asyncio.run(args.func(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001 - one clean line instead of a traceback
        print(f"tower {args.command}: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
