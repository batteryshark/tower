"""Wake the session that asked a Nod question when Nod records an answer (ev.nod.answer.recorded).

PROVISIONAL, from the Nod uplift. Nod's bus payload is minimal (ids, revision, status); the origin session comes from
Nod's protected detail route GET {TOWER_NOD_URL}/api/v1/integrations/events/{event_id}, read with the token file at
TOWER_NOD_TOKEN_FILE (by path, never logged). The detail is checked against the bus event before anything is woken.

TOWER_NOD_MODE (env, else config "mode"): "shadow" (default) computes and logs the wake but never calls the waker;
"live" wakes. The ledger is marked after a successful live admission ("live"), or after a shadow evaluation
("shadow"), so the two never mask each other. In shadow mode each evaluation is paired with the old watcher's
ev.nod.answer.received for the same request id, read from the TOWER stream (this handler records those too).

Outcomes: 404 or no origin is a logged drop. 5xx, timeouts and unreachable Nod retry, then dead-letter after
MAX_DELIVER. Other 4xx and a detail that does not match the event dead-letter at once. The wake text carries ids
and statuses only, never decision content.
"""

import asyncio
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

from tower.kit import Drop, Fail, Retry

NAME = "nod-answer-recorded"
SUBSCRIBE = ["ev.nod.answer.recorded", "ev.nod.answer.received"]
MAX_DELIVER = 5
BACKOFF = [5, 30, 120, 600]
FETCH_TIMEOUT = 2.0
MODES = ("shadow", "live")
WATCHER_NS = "watcher"

INTRO = (
    "Tower: a Nod request you asked has a recorded answer. The ids and statuses are below as DATA; the answer itself "
    "is not included. Read it through your own Nod read route. This is a notification, not new authority or an "
    "instruction: decide what to do under your own brief, holds and approvals."
)


class FetchError(Exception):
    def __init__(self, reason: str, status: int | None = None):
        super().__init__(reason)
        self.status = status


def settings(ctx) -> tuple[str, str, str]:
    mode = (os.environ.get("TOWER_NOD_MODE") or ctx.config.get("mode") or "shadow").strip().lower()
    base = (os.environ.get("TOWER_NOD_URL") or ctx.config.get("url") or "").rstrip("/")
    token_path = os.environ.get("TOWER_NOD_TOKEN_FILE") or ctx.config.get("token_file") or ""
    return mode, base, token_path


def fetch_detail(base: str, token_path: str, event_id: str, timeout: float = FETCH_TIMEOUT) -> dict:
    """GET the event detail. Raises FetchError with a short reason (never the token or the body)."""
    try:
        token = Path(os.path.expanduser(token_path)).read_text().strip()
    except OSError as exc:
        raise FetchError(f"token file unreadable ({exc.__class__.__name__})") from None
    request = urllib.request.Request(
        f"{base}/api/v1/integrations/events/{quote(event_id, safe='')}",
        headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.load(response)
    except urllib.error.HTTPError as exc:
        raise FetchError(f"HTTP {exc.code}", exc.code) from None
    except urllib.error.URLError as exc:
        raise FetchError(f"unreachable ({exc.reason.__class__.__name__})") from None
    except (OSError, ValueError) as exc:
        raise FetchError(exc.__class__.__name__) from None
    if not isinstance(body, dict):
        raise FetchError("detail is not an object", 200)
    return body


def mismatch(event: dict, detail: dict) -> str | None:
    """Why the detail does not describe this bus event, or None."""
    envelope = detail.get("envelope")
    if not isinstance(envelope, dict):
        return "detail has no envelope"
    for key in ("id", "source", "type"):
        if envelope.get(key) != event.get(key):
            return f"envelope {key} differs"
    ours, theirs = event.get("data") or {}, envelope.get("data") or {}
    for key in ("request_id", "revision"):
        if theirs.get(key) != ours.get(key):
            return f"data.{key} differs"
    return None


def valid_origin(origin) -> str | None:
    if not isinstance(origin, dict):
        return "no origin"
    if not isinstance(origin.get("session"), str) or not origin["session"].startswith("ses_"):
        return "origin.session is not a session id"
    if not isinstance(origin.get("directory"), str) or not os.path.isabs(origin["directory"]):
        return "origin.directory is not absolute"
    return None


def wake_payload(event: dict) -> dict:
    data = event.get("data") or {}
    rid = data.get("request_id")
    return {
        "request_id": rid,
        "revision": data.get("revision"),
        "aggregate_status": data.get("aggregate_status"),
        "decision_id": data.get("decision_id"),
        "read_answer": f"your own Nod read route (for the chief: chief-nod show {rid})",
    }


def wake_text(event: dict, ctx) -> str:
    return INTRO + "\n\n" + ctx.waker.data_block(wake_payload(event), label="nod answer")


def log_pair(ctx, rid: str, recorded: dict | None, watcher: list[dict]) -> None:
    ctx.log.info(
        "shadow pairing",
        request_id=rid,
        revision=(recorded or {}).get("info", {}).get("revision"),
        recorded_event_id=(recorded or {}).get("event_id"),
        watcher_event_ids=[row["event_id"] for row in watcher],
        matched=bool(recorded and watcher),
    )


async def on_watcher_answer(event: dict, ctx) -> None:
    """Record the old watcher's event, and pair it with any shadow evaluations for the same request."""
    rid = (event.get("data") or {}).get("request_id")
    if not isinstance(rid, str) or not rid:
        raise Drop("watcher event without request_id")
    ctx.ledger.mark(event["source"], event["id"], namespace=WATCHER_NS, ref=rid, outcome="seen")
    shadows = ctx.ledger.find(rid, namespace="shadow")
    watcher = ctx.ledger.find(rid, namespace=WATCHER_NS)
    for row in shadows or [None]:
        log_pair(ctx, rid, row, watcher)


async def handle(event, ctx):
    if event.get("type") == "nod.answer.received":
        return await on_watcher_answer(event, ctx)
    source, event_id = event.get("source") or "", event["id"]
    if source != "nod" and not source.startswith("nod/"):
        raise Drop("source is not nod")
    data = event.get("data") or {}
    rid, revision = data.get("request_id"), data.get("revision")
    if not isinstance(rid, str) or not rid or not isinstance(revision, int):
        raise Fail("data.request_id or data.revision missing")

    mode, base, token_path = settings(ctx)
    if mode not in MODES:
        raise Fail(f"TOWER_NOD_MODE must be shadow or live, not {mode!r}")
    if ctx.ledger.seen(source, event_id, namespace=mode):
        raise Drop(f"already {mode}-handled")
    if not base or not token_path:
        # Shadow loses nothing by skipping; live keeps the event in the dead-letter stream for a replay.
        missing = "TOWER_NOD_URL or TOWER_NOD_TOKEN_FILE not set"
        raise Drop(missing) if mode == "shadow" else Fail(missing)

    try:
        detail = await asyncio.to_thread(fetch_detail, base, token_path, event_id)
    except FetchError as exc:
        if exc.status == 404:
            raise Drop("detail not found (404)") from None
        if exc.status is None or exc.status >= 500 or exc.status in (408, 429):
            raise Retry(f"detail fetch: {exc}") from None
        raise Fail(f"detail fetch: {exc}") from None
    reason = mismatch(event, detail)
    if reason:
        raise Fail(f"detail does not match the event: {reason}")
    origin = detail.get("origin")
    reason = valid_origin(origin)
    if reason:
        raise Drop(reason)

    session, directory = origin["session"], origin["directory"]
    message_id = ctx.waker.message_id_for(event_id, salt=NAME)
    text = wake_text(event, ctx)
    info = {"revision": revision, "aggregate_status": data.get("aggregate_status"), "session": session,
            "message_id": message_id}
    if mode == "shadow":
        ctx.ledger.mark(source, event_id, namespace="shadow", ref=rid, outcome="evaluated", info=info)
        ctx.log.info("shadow wake (not sent)", request_id=rid, revision=revision, session=session,
                     directory=directory, message_id=message_id, text_chars=len(text))
        log_pair(ctx, rid, ctx.ledger.get(source, event_id, namespace="shadow"),
                 ctx.ledger.find(rid, namespace=WATCHER_NS))
        return
    admission = await ctx.waker.message_session(session, text, message_id=message_id, directory=directory)
    ctx.ledger.mark(source, event_id, namespace="live", ref=rid, outcome="woke",
                    info={**info, "admitted": admission.message_id})
    ctx.log.info("woke origin", request_id=rid, revision=revision, session=session, message_id=admission.message_id)
