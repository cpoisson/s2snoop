"""SQLite persistence: sessions, events (audio stripped) and probe series.

Kept independent from the web layer so another front end (e.g. Gradio) can
read the same database.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def loads(raw: str | bytes):
    """json.loads that maps the non-standard NaN / Infinity tokens to None.

    Python's json.dumps writes them by default (s2s uses float("inf") as a
    setup value), but strict encoders downstream (Starlette, JSON.parse) reject them.
    """
    return json.loads(raw, parse_constant=lambda _: None)


SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    started_at REAL NOT NULL,
    ended_at REAL,
    meta TEXT NOT NULL DEFAULT '{}',
    stats TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    t REAL NOT NULL,
    source TEXT NOT NULL,
    type TEXT,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_session ON events(session_id, id);
CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    t REAL NOT NULL,
    kind TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS series_session ON series(session_id, seq);
"""


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self._close_dangling()

    def _close_dangling(self) -> None:
        """Sessions left open by a previous run (crash, Ctrl+C) are marked as ended."""
        rows = self.db.execute("SELECT id, started_at, meta, stats FROM sessions WHERE ended_at IS NULL").fetchall()
        for sid, started, meta, stats in rows:
            meta = loads(meta)
            meta.setdefault("close_reason", "s2snoop stopped during the session")
            ended = started + float(loads(stats).get("duration") or 0)
            self.db.execute("UPDATE sessions SET ended_at=?, meta=? WHERE id=?", (ended, json.dumps(meta), sid))
        self.db.commit()

    # ------------------------------------------------------------ writes
    def create_session(self, sid: str, started_at: float, meta: dict) -> None:
        self.db.execute("INSERT OR REPLACE INTO sessions(id, started_at, meta) VALUES (?, ?, ?)",
                        (sid, started_at, json.dumps(meta)))
        self.db.commit()

    def update_session(self, sid: str, meta: dict, stats: dict, ended_at: float | None) -> None:
        self.db.execute("UPDATE sessions SET meta=?, stats=?, ended_at=? WHERE id=?",
                        (json.dumps(meta), json.dumps(stats), ended_at, sid))

    def add_events(self, rows: list[tuple[str, float, str, str | None, str]]) -> None:
        if rows:
            self.db.executemany("INSERT INTO events(session_id, t, source, type, data) VALUES (?,?,?,?,?)", rows)

    def add_series(self, rows: list[tuple[str, int, float, str, str]]) -> None:
        if rows:
            self.db.executemany("INSERT INTO series(session_id, seq, t, kind, data) VALUES (?,?,?,?,?)", rows)

    def commit(self) -> None:
        self.db.commit()

    def delete_session(self, sid: str) -> None:
        for table, col in (("events", "session_id"), ("series", "session_id"), ("sessions", "id")):
            self.db.execute(f"DELETE FROM {table} WHERE {col}=?", (sid,))
        self.db.commit()

    # ------------------------------------------------------------ reads
    def list_sessions(self, limit: int = 200) -> list[dict]:
        cur = self.db.execute(
            "SELECT id, started_at, ended_at, meta, stats FROM sessions ORDER BY started_at DESC LIMIT ?", (limit,))
        return [{"id": r[0], "started_at": r[1], "ended_at": r[2], "meta": loads(r[3]),
                 "stats": loads(r[4])} for r in cur.fetchall()]

    def get_session_row(self, sid: str) -> dict | None:
        r = self.db.execute("SELECT id, started_at, ended_at, meta, stats FROM sessions WHERE id=?",
                            (sid,)).fetchone()
        if not r:
            return None
        return {"id": r[0], "started_at": r[1], "ended_at": r[2], "meta": loads(r[3]), "stats": loads(r[4])}

    def iter_events(self, sid: str):
        cur = self.db.execute("SELECT t, source, data FROM events WHERE session_id=? ORDER BY id", (sid,))
        for t, source, data in cur:
            yield t, source, loads(data)

    def events_page(self, sid: str, offset: int, limit: int, type_filter: str | None, source: str | None) -> dict:
        where, args = ["session_id=?"], [sid]
        if type_filter:
            where.append("type LIKE ?")
            args.append(f"%{type_filter}%")
        if source:
            where.append("source=?")
            args.append(source)
        clause = " AND ".join(where)
        total = self.db.execute(f"SELECT COUNT(*) FROM events WHERE {clause}", args).fetchone()[0]
        cur = self.db.execute(f"SELECT id, t, source, type, data FROM events WHERE {clause} ORDER BY id LIMIT ? OFFSET ?",
                              [*args, limit, offset])
        rows = [{"id": r[0], "t": r[1], "source": r[2], "type": r[3], "data": loads(r[4])} for r in cur]
        return {"total": total, "rows": rows}

    def series_since(self, sid: str, after_seq: int) -> list[dict]:
        cur = self.db.execute("SELECT seq, t, kind, data FROM series WHERE session_id=? AND seq>? ORDER BY seq",
                              (sid, after_seq))
        return [{"seq": r[0], "t": r[1], "kind": r[2], "data": loads(r[3])} for r in cur]

    def sessions_older_than(self, epoch: float) -> list[str]:
        cur = self.db.execute("SELECT id FROM sessions WHERE started_at < ?", (epoch,))
        return [r[0] for r in cur]
