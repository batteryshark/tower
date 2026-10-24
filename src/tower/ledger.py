"""A persistent ledger of events a handler has dealt with, keyed on (source, event id).

One SQLite file, ``ledger.sqlite`` under the state root (default ~/.tower/state), shared by every handler. Rows are
scoped by handler name and a namespace, so a handler can keep separate books (for example "live" and "shadow"):

    ledger = ctx.ledger                      # bound to this handler
    if ledger.seen(event["source"], event["id"], namespace="live"):
        raise kit.Drop("already handled")
    ...act...
    ledger.mark(event["source"], event["id"], namespace="live", ref=request_id, outcome="woke", info={...})

``ref`` is an optional lookup key (a request id, a PR number) for ``find``. ``info`` is small JSON for operators:
never put secrets or event content in it. Rows are never expired automatically.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

from . import events

FILENAME = "ledger.sqlite"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger (
    handler   TEXT NOT NULL,
    namespace TEXT NOT NULL,
    source    TEXT NOT NULL,
    event_id  TEXT NOT NULL,
    ref       TEXT,
    outcome   TEXT NOT NULL,
    info      TEXT NOT NULL DEFAULT '{}',
    at        TEXT NOT NULL,
    PRIMARY KEY (handler, namespace, source, event_id)
);
CREATE INDEX IF NOT EXISTS ledger_ref ON ledger (handler, namespace, ref);
"""


class Ledger:
    """The shared ledger file. Thread-safe; each call is one short transaction."""

    def __init__(self, path: Path | str):
        self.path = Path(os.path.expanduser(str(path)))
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fresh = not self.path.exists()
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=10)
        if fresh:
            os.chmod(self.path, 0o600)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._lock = threading.Lock()

    @classmethod
    def under(cls, state_root: Path | str) -> "Ledger":
        return cls(Path(os.path.expanduser(str(state_root))) / FILENAME)

    def bind(self, handler: str) -> "HandlerLedger":
        return HandlerLedger(self, handler)

    def close(self) -> None:
        self._db.close()

    def _get(self, handler: str, namespace: str, source: str, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute(
                "SELECT ref, outcome, info, at FROM ledger WHERE handler=? AND namespace=? AND source=? AND event_id=?",
                (handler, namespace, source, event_id),
            ).fetchone()
        if row is None:
            return None
        return {"source": source, "event_id": event_id, "ref": row[0], "outcome": row[1],
                "info": json.loads(row[2]), "at": row[3]}

    def _mark(self, handler: str, namespace: str, source: str, event_id: str, *, ref: str | None,
              outcome: str, info: dict[str, Any] | None) -> bool:
        """Insert once; returns False if the row already existed (the first mark wins)."""
        with self._lock:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO ledger (handler, namespace, source, event_id, ref, outcome, info, at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (handler, namespace, source, event_id, ref, outcome,
                 json.dumps(info or {}, sort_keys=True, default=str), events.now_iso()),
            )
            return cur.rowcount == 1

    def _find(self, handler: str, namespace: str, ref: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT source, event_id, outcome, info, at FROM ledger WHERE handler=? AND namespace=? AND ref=? "
                "ORDER BY at", (handler, namespace, ref),
            ).fetchall()
        return [{"source": r[0], "event_id": r[1], "ref": ref, "outcome": r[2], "info": json.loads(r[3]), "at": r[4]}
                for r in rows]

    def event_ids(self, handler: str, namespaces: tuple[str, ...]) -> set[tuple[str, str]]:
        """Every (source, event_id) marked by handler in any of namespaces."""
        marks = ",".join("?" * len(namespaces))
        with self._lock:
            rows = self._db.execute(
                f"SELECT source, event_id FROM ledger WHERE handler=? AND namespace IN ({marks})",
                (handler, *namespaces),
            ).fetchall()
        return {(r[0], r[1]) for r in rows}


class HandlerLedger:
    """The ledger as one handler sees it."""

    def __init__(self, ledger: Ledger, handler: str):
        self._ledger = ledger
        self.handler = handler

    def get(self, source: str, event_id: str, *, namespace: str = "default") -> dict[str, Any] | None:
        return self._ledger._get(self.handler, namespace, source, event_id)

    def seen(self, source: str, event_id: str, *, namespace: str = "default") -> bool:
        return self.get(source, event_id, namespace=namespace) is not None

    def mark(self, source: str, event_id: str, *, namespace: str = "default", ref: str | None = None,
             outcome: str = "done", info: dict[str, Any] | None = None) -> bool:
        return self._ledger._mark(self.handler, namespace, source, event_id, ref=ref, outcome=outcome, info=info)

    def find(self, ref: str, *, namespace: str = "default") -> list[dict[str, Any]]:
        return self._ledger._find(self.handler, namespace, ref)
