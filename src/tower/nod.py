"""`tower nod`: report-only helpers for the Nod uplift (provisional).

    tower nod reconcile --after CURSOR [--limit N]   nod.answer.recorded events in Nod's journal missing from the ledger
    tower nod pending                                open request ids in the old watcher's ledger (ids only)

reconcile pages GET {TOWER_NOD_URL}/api/v1/integrations/events?after=&limit=&through= up to the high water fixed by the
first page, with the token at TOWER_NOD_TOKEN_FILE (by path, never printed). It reads and reports; it never publishes.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlencode

from . import kit

HANDLER = "nod-answer-recorded"
WATCHER = "nod-watcher"
ANSWER_TYPE = "nod.answer.recorded"
LIST_TIMEOUT = 10.0


class NodError(Exception):
    pass


def _settings() -> tuple[str, str]:
    config = kit.load_config().get(HANDLER) or {}
    base = (os.environ.get("TOWER_NOD_URL") or config.get("url") or "").rstrip("/")
    token_path = os.environ.get("TOWER_NOD_TOKEN_FILE") or config.get("token_file") or ""
    if not base or not token_path:
        raise NodError("TOWER_NOD_URL and TOWER_NOD_TOKEN_FILE must be set")
    return base, token_path


def get_page(base: str, token_path: str, after: int, limit: int, through: int | None) -> dict[str, Any]:
    try:
        token = Path(os.path.expanduser(token_path)).read_text().strip()
    except OSError as exc:
        raise NodError(f"token file unreadable ({exc.__class__.__name__})") from None
    query = {"after": after, "limit": limit, **({"through": through} if through is not None else {})}
    request = urllib.request.Request(
        f"{base}/api/v1/integrations/events?{urlencode(query)}",
        headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=LIST_TIMEOUT) as response:
            body = json.load(response)
    except urllib.error.HTTPError as exc:
        raise NodError(f"list failed: HTTP {exc.code}") from None
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise NodError(f"list failed: {exc.__class__.__name__}") from None
    if not isinstance(body, dict) or not isinstance(body.get("events"), list):
        raise NodError("list response has no events")
    return body


def scan(after: int, limit: int, fetch: Callable[..., dict[str, Any]]) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield (seq, envelope) from after up to the first page's high water; the last yielded item is the cursor."""
    through = None
    cursor = after
    while True:
        page = fetch(cursor, limit, through)
        if through is None:
            through = int(page.get("high_water") or 0)
        for item in page["events"]:
            yield int(item.get("seq") or 0), item.get("envelope") or {}
        next_cursor = int(page.get("next_cursor") or cursor)
        if next_cursor <= cursor or next_cursor >= through:
            yield next_cursor, {}
            return
        cursor = next_cursor


def reconcile(after: int, limit: int, *, state_root: Path | str = kit.STATE_ROOT,
              fetch: Callable[..., dict[str, Any]] | None = None) -> dict[str, Any]:
    if fetch is None:
        base, token_path = _settings()
        fetch = lambda a, l, t: get_page(base, token_path, a, l, t)  # noqa: E731
    known = kit.shared_ledger(state_root).event_ids(HANDLER, ("live", "shadow"))
    scanned, answers, missing, cursor = 0, 0, [], after
    for seq, envelope in scan(after, limit, fetch):
        if not envelope:
            cursor = max(cursor, seq)
            continue
        scanned += 1
        cursor = max(cursor, seq)
        if envelope.get("type") != ANSWER_TYPE:
            continue
        answers += 1
        if (envelope.get("source"), envelope.get("id")) not in known:
            data = envelope.get("data") or {}
            missing.append({"seq": seq, "id": envelope.get("id"), "request_id": data.get("request_id"),
                            "revision": data.get("revision")})
    return {"scanned": scanned, "answers": answers, "missing": missing, "next_cursor": cursor}


def pending(state_root: Path | str = kit.STATE_ROOT) -> list[str]:
    path = Path(os.path.expanduser(str(state_root))) / WATCHER / "requests.json"
    try:
        rows = json.loads(path.read_text())
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError) as exc:
        raise NodError(f"watcher ledger unreadable ({exc.__class__.__name__})") from None
    return sorted(rid for rid, row in rows.items() if isinstance(row, dict) and row.get("state") == "open")
