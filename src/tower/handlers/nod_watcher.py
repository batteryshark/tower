"""Nod watcher: wait on Nod requests announced on the bus and publish their answers.

Consumes ev.nod.request.asked (data: request_id, origin {session, directory}, optional reference) with its own
durable consumer, keeps open requests in ~/.tower/state/nod-watcher/requests.json, polls Nod
GET /api/v1/requests/{id}/decision for each, and publishes ev.nod.answer.received once per answered request.
The event id is derived from the request id and an answer digest, so a restart never double-publishes.

Env: TOWER_NOD_URL (Nod base URL), TOWER_NOD_TOKEN_FILE (Bearer token, read by path). Both are required: without
either, the watcher logs "disabled" and idles. Read-only GETs, only for announced request ids.
Never logs answers, request bodies or the token.
"""

import asyncio
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

NAME = "nod-watcher"
TERMINAL = {"cancelled", "canceled", "expired", "withdrawn"}
MAX_TEXT = 1000


class FetchError(Exception):
    pass


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Ledger:
    """Small JSON file of announced requests: rid -> {origin, reference, state, ...}. 0600, atomic writes."""

    def __init__(self, path: Path):
        self.path = path
        try:
            self.rows = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            self.rows = {}

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(self.rows, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)

    def open_rows(self):
        return [(rid, row) for rid, row in self.rows.items() if row.get("state") == "open"]


def fetch_decision(base: str, token_path: str, rid: str, timeout: float = 15) -> dict:
    try:
        token = Path(os.path.expanduser(token_path)).read_text().strip()
    except OSError as exc:
        raise FetchError(f"token file unreadable ({exc.__class__.__name__})") from None
    request = urllib.request.Request(
        f"{base}/api/v1/requests/{quote(rid, safe='')}/decision",
        headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.load(response)
    except urllib.error.HTTPError as exc:
        raise FetchError(f"HTTP {exc.code}") from None
    except urllib.error.URLError as exc:
        raise FetchError(f"URLError: {exc.reason}") from None
    except (OSError, ValueError) as exc:
        raise FetchError(exc.__class__.__name__) from None
    if not isinstance(body, dict) or body.get("request_id") != rid:
        raise FetchError("decision request identity mismatch")
    return body


def summarize(choice: dict) -> dict:
    text = choice.get("text")
    return {
        "option_id": choice.get("option_id"),
        "label": choice.get("option_label") or choice.get("label"),
        "text": text[:MAX_TEXT] if isinstance(text, str) else None,
        "answered_at": choice.get("decided_at") or choice.get("created_at"),
    }


def answer_event(rid: str, row: dict, body: dict):
    """(event_id, data) if the request is answered, else None."""
    answers = list(body.get("decisions") or [])
    if body.get("decision") and body["decision"] not in answers:
        answers.append(body["decision"])
    if not answers:
        return None
    semantic = {k: body.get(k) for k in ("status", "decision", "decision_resolution", "request_digest")}
    semantic["decisions"] = sorted(answers, key=digest)
    event_id = "nod-answer-" + hashlib.sha256(f"{rid}\x1f{digest(semantic)}".encode()).hexdigest()[:32]
    chosen = body.get("decision") or answers[0]
    data = {
        "request_id": rid,
        "origin": row["origin"],
        "status": body.get("status"),
        "answer": summarize(chosen) if isinstance(chosen, dict) else None,
        "answers": len(answers),
    }
    if row.get("reference"):
        data["reference"] = row["reference"]
    return event_id, data


def valid_request(data) -> str | None:
    """Reason the ev.nod.request.asked data is unusable, or None."""
    rid = data.get("request_id")
    origin = data.get("origin") or {}
    if not isinstance(rid, str) or not rid or len(rid) > 256:
        return "request_id missing"
    if not isinstance(origin.get("session"), str) or not origin["session"].startswith("ses_"):
        return "origin.session missing"
    if not isinstance(origin.get("directory"), str) or not os.path.isabs(origin["directory"]):
        return "origin.directory must be absolute"
    return None


async def intake(ctx, ledger: Ledger) -> None:
    async for event, msg in ctx.subscribe("ev.nod.request.asked"):
        if event is None:
            ctx.log.warning("invalid envelope", seq=msg.metadata.sequence.stream)
            await msg.term()
            continue
        data = event.get("data") or {}
        reason = valid_request(data if isinstance(data, dict) else {})
        if reason:
            ctx.log.warning("ignored request", id=event["id"], reason=reason)
            await msg.term()
            continue
        rid = data["request_id"]
        if rid in ledger.rows:
            ctx.log.info("request already known", request_id=rid, state=ledger.rows[rid].get("state"))
        else:
            origin = {"session": data["origin"]["session"], "directory": data["origin"]["directory"]}
            ledger.rows[rid] = {
                "origin": origin,
                "reference": data.get("reference") if isinstance(data.get("reference"), str) else None,
                "asked_event_id": event["id"],
                "asked_at": time.time(),
                "state": "open",
                "failures": 0,
                "next_check": 0,
            }
            ledger.save()
            ctx.log.info("watching request", request_id=rid, session=origin["session"])
        await msg.ack()


async def poll(ctx, ledger: Ledger, base: str, token_path: str) -> None:
    interval = float(ctx.config.get("poll_interval") or 15)
    max_age = float(ctx.config.get("max_age_days") or 14) * 86400
    while True:
        now = time.time()
        for rid, row in ledger.open_rows():
            if row.get("next_check", 0) > now:
                continue
            if now - row.get("asked_at", now) > max_age:
                row["state"] = "expired"
                ledger.save()
                ctx.log.info("stopped watching (too old)", request_id=rid)
                continue
            try:
                body = await asyncio.to_thread(fetch_decision, base, token_path, rid)
            except FetchError as exc:
                row["failures"] = row.get("failures", 0) + 1
                row["next_check"] = now + min(300, interval * 2 ** min(row["failures"], 5))
                row["fetch_error"] = str(exc)
                ledger.save()
                ctx.log.warning("fetch failed", request_id=rid, error=str(exc), failures=row["failures"])
                continue
            row["failures"], row["next_check"] = 0, now + interval
            row.pop("fetch_error", None)
            answered = answer_event(rid, row, body)
            if answered:
                event_id, data = answered
                await ctx.publish("nod.answer.received", data, (), event_id, subject=rid, source="tower/watcher/nod")
                row.update(state="answered", answer_event_id=event_id, answered_seen_at=now)
                ctx.log.info("answer published", request_id=rid, event_id=event_id)
            elif str(body.get("status") or "").lower() in TERMINAL:
                row["state"] = "closed"
                ctx.log.info("request closed without answer", request_id=rid, status=body.get("status"))
            ledger.save()
        await asyncio.sleep(interval)


async def run(ctx):
    token_path = os.environ.get("TOWER_NOD_TOKEN_FILE") or ctx.config.get("token_file")
    base = (os.environ.get("TOWER_NOD_URL") or ctx.config.get("url") or "").rstrip("/")
    if not token_path or not base:
        missing = [n for n, v in (("TOWER_NOD_URL", base), ("TOWER_NOD_TOKEN_FILE", token_path)) if not v]
        ctx.log.info("disabled", reason=f"{' and '.join(missing)} not set")
        await asyncio.Event().wait()
    ledger = Ledger(ctx.state_dir / "requests.json")
    ctx.log.info("watching", nod=base, open=len(ledger.open_rows()))
    await asyncio.gather(intake(ctx, ledger), poll(ctx, ledger, base, token_path))
