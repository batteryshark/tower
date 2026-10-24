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

``mark`` inserts once. ``record`` keeps one row per event as a STATE that moves forward (``retrying:<reason>`` until a
terminal ``handled``, ``already_admitted``, ``dropped:<reason>`` or ``dead_lettered:<reason>``), counting attempts; a
terminal row is only replaced on request (an operator retry). ``claim`` is a short lease on a key, so two paths
(bus and catch-up, in one or two processes) never act on the same event at once. ``get_value``/``set_value`` keep
small JSON values such as a cursor.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from . import events

FILENAME = "ledger.sqlite"
TERMINAL = ("handled", "already_admitted", "dropped", "dead_lettered")


def is_terminal(state: str | None) -> bool:
    return bool(state) and state.split(":", 1)[0] in TERMINAL

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
CREATE TABLE IF NOT EXISTS claims (key TEXT PRIMARY KEY, owner TEXT NOT NULL, until REAL NOT NULL);
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, at TEXT NOT NULL);
"""
_COLUMNS = (("attempts", "INTEGER NOT NULL DEFAULT 0"), ("first_at", "TEXT"))


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
        have = {row[1] for row in self._db.execute("PRAGMA table_info(ledger)")}
        for name, decl in _COLUMNS:
            if name not in have:
                self._db.execute(f"ALTER TABLE ledger ADD COLUMN {name} {decl}")
        self._lock = threading.Lock()

    @classmethod
    def under(cls, state_root: Path | str) -> "Ledger":
        return cls(Path(os.path.expanduser(str(state_root))) / FILENAME)

    def bind(self, handler: str) -> "HandlerLedger":
        return HandlerLedger(self, handler)

    def close(self) -> None:
        self._db.close()

    _FIELDS = "namespace, source, event_id, ref, outcome, info, at, attempts, first_at"

    @staticmethod
    def _row(r) -> dict[str, Any]:
        return {"namespace": r[0], "source": r[1], "event_id": r[2], "ref": r[3], "outcome": r[4],
                "info": json.loads(r[5]), "at": r[6], "attempts": r[7], "first_at": r[8] or r[6]}

    def _get(self, handler: str, namespace: str, source: str, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute(
                f"SELECT {self._FIELDS} FROM ledger WHERE handler=? AND namespace=? AND source=? AND event_id=?",
                (handler, namespace, source, event_id),
            ).fetchone()
        return None if row is None else self._row(row)

    def _by_event(self, handler: str, namespace: str, event_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                f"SELECT {self._FIELDS} FROM ledger WHERE handler=? AND namespace=? AND event_id=?",
                (handler, namespace, event_id),
            ).fetchall()
        return [self._row(r) for r in rows]

    def _record(self, handler: str, namespace: str, source: str, event_id: str, *, state: str, ref: str | None,
                info: dict[str, Any] | None, attempt: bool, replace_terminal: bool) -> dict[str, Any]:
        """Upsert the event's state; returns the row as stored (unchanged if terminal and not replace_terminal)."""
        now = events.now_iso()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    "SELECT outcome, info, attempts FROM ledger WHERE handler=? AND namespace=? AND source=? "
                    "AND event_id=?", (handler, namespace, source, event_id)).fetchone()
                if row is None:
                    self._db.execute(
                        "INSERT INTO ledger (handler, namespace, source, event_id, ref, outcome, info, at, attempts, "
                        "first_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (handler, namespace, source, event_id, ref, state,
                         json.dumps(info or {}, sort_keys=True, default=str), now, int(attempt), now))
                elif replace_terminal or not is_terminal(row[0]):
                    merged = {**json.loads(row[1]), **(info or {})}
                    self._db.execute(
                        "UPDATE ledger SET outcome=?, info=?, at=?, attempts=attempts+?, ref=COALESCE(?, ref) "
                        "WHERE handler=? AND namespace=? AND source=? AND event_id=?",
                        (state, json.dumps(merged, sort_keys=True, default=str), now, int(attempt), ref,
                         handler, namespace, source, event_id))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return self._get(handler, namespace, source, event_id)

    def rows(self, handler: str, *, exclude: tuple[str, ...] = (), limit: int = 20) -> list[dict[str, Any]]:
        """Latest rows first, skipping states (by prefix) in exclude."""
        marks = " AND ".join("outcome NOT LIKE ?" for _ in exclude) or "1"
        with self._lock:
            rows = self._db.execute(
                f"SELECT {self._FIELDS} FROM ledger WHERE handler=? AND {marks} ORDER BY at DESC LIMIT ?",
                (handler, *(f"{e}%" for e in exclude), limit)).fetchall()
        return [self._row(r) for r in rows]

    def counts(self, handler: str) -> dict[str, dict[str, int]]:
        """{namespace: {state kind: n}}; the kind is the state before any ':reason'."""
        with self._lock:
            rows = self._db.execute("SELECT namespace, outcome FROM ledger WHERE handler=?", (handler,)).fetchall()
        out: dict[str, dict[str, int]] = {}
        for namespace, outcome in rows:
            kind = outcome.split(":", 1)[0]
            out.setdefault(namespace, {}).setdefault(kind, 0)
            out[namespace][kind] += 1
        return out

    # -- claims and values ------------------------------------------------

    def claim(self, key: str, ttl: float = 120.0) -> str | None:
        """A lease token on key, or None while someone else holds an unexpired lease."""
        token, now = secrets.token_hex(8), time.time()
        with self._lock:
            cur = self._db.execute(
                "INSERT INTO claims (key, owner, until) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET owner=excluded.owner, until=excluded.until WHERE claims.until < ?",
                (key, token, now + ttl, now))
        return token if cur.rowcount == 1 else None

    def release(self, key: str, token: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM claims WHERE key=? AND owner=?", (key, token))

    def get_value(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return default if row is None else json.loads(row[0])

    def set_value(self, key: str, value: Any) -> None:
        with self._lock:
            self._db.execute("INSERT INTO kv (key, value, at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET "
                             "value=excluded.value, at=excluded.at", (key, json.dumps(value, default=str),
                                                                     events.now_iso()))

    def _mark(self, handler: str, namespace: str, source: str, event_id: str, *, ref: str | None,
              outcome: str, info: dict[str, Any] | None) -> bool:
        """Insert once; returns False if the row already existed (the first mark wins)."""
        with self._lock:
            now = events.now_iso()
            cur = self._db.execute(
                "INSERT OR IGNORE INTO ledger (handler, namespace, source, event_id, ref, outcome, info, at, first_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (handler, namespace, source, event_id, ref, outcome,
                 json.dumps(info or {}, sort_keys=True, default=str), now, now),
            )
            return cur.rowcount == 1

    def _find(self, handler: str, namespace: str, ref: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT source, event_id, outcome, info, at FROM ledger WHERE handler=? AND namespace=? AND ref=? "
                "ORDER BY at", (handler, namespace, ref),
            ).fetchall()
        return [{"source": r[0], "event_id": r[1], "ref": ref, "outcome": r[2], "info": json.loads(r[3]), "at": r[4]}
                for r in rows]  # ordered oldest first

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

    def by_event(self, event_id: str, *, namespace: str = "default") -> list[dict[str, Any]]:
        return self._ledger._by_event(self.handler, namespace, event_id)

    def record(self, source: str, event_id: str, *, namespace: str = "default", state: str, ref: str | None = None,
               info: dict[str, Any] | None = None, attempt: bool = True,
               replace_terminal: bool = False) -> dict[str, Any]:
        return self._ledger._record(self.handler, namespace, source, event_id, state=state, ref=ref, info=info,
                                    attempt=attempt, replace_terminal=replace_terminal)

    def rows(self, *, exclude: tuple[str, ...] = (), limit: int = 20) -> list[dict[str, Any]]:
        return self._ledger.rows(self.handler, exclude=exclude, limit=limit)

    def counts(self) -> dict[str, dict[str, int]]:
        return self._ledger.counts(self.handler)
