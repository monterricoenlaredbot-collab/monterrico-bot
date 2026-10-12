"""Persistencia local para demos, PostgreSQL (Supabase) en el servidor.
Nunca se conecta a WhatsApp: los mensajes quedan en cola ('queued').
"""
from __future__ import annotations
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

CREATE_SQLITE = """
CREATE TABLE IF NOT EXISTS observed_races (
  race_id INTEGER PRIMARY KEY,
  card_id INTEGER NOT NULL,
  first_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS result_outbox (
  race_id INTEGER PRIMARY KEY,
  card_id INTEGER NOT NULL,
  message TEXT NOT NULL,
  digest TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'queued',
  queued_at TEXT NOT NULL
);
"""

class SQLiteStore:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(path)
        self.conn.executescript(CREATE_SQLITE)

    def is_observed(self, race_id: int) -> bool:
        return self.conn.execute("SELECT 1 FROM observed_races WHERE race_id=?", (race_id,)).fetchone() is not None

    def mark_observed(self, race_id: int, card_id: int):
        self.conn.execute("INSERT OR IGNORE INTO observed_races(race_id,card_id,first_seen) VALUES(?,?,?)",
                          (race_id,card_id,datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    def existing_digest(self, race_id: int):
        result = self.conn.execute("SELECT digest FROM result_outbox WHERE race_id=?", (race_id,)).fetchone()
        return result[0] if result else None

    def insert_outbox(self, race_id: int, card_id: int, message: str, digest: str) -> bool:
        cur = self.conn.execute("INSERT OR IGNORE INTO result_outbox(race_id,card_id,message,digest,queued_at) VALUES(?,?,?,?,?)",
                                (race_id,card_id,message,digest,datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
        return cur.rowcount == 1


class PostgresStore:
    def __init__(self, url: str):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("Falta psycopg; instala requirements.txt") from exc
        self.psycopg = psycopg
        self.url = url
        self.conn = None
        self._ensure_conn()

    def _ensure_conn(self):
        if self.conn is None or self.conn.closed:
            # Supabase connection string for session pooler or direct Postgres.
            self.conn = self.psycopg.connect(self.url, autocommit=True, connect_timeout=12)

    def is_observed(self, race_id: int) -> bool:
        self._ensure_conn()
        return self.conn.execute("SELECT 1 FROM observed_races WHERE race_id=%s", (race_id,)).fetchone() is not None

    def mark_observed(self, race_id: int, card_id: int):
        self._ensure_conn()
        self.conn.execute("INSERT INTO observed_races(race_id,card_id,first_seen) VALUES(%s,%s,NOW()) ON CONFLICT (race_id) DO NOTHING", (race_id,card_id))

    def existing_digest(self, race_id: int):
        self._ensure_conn()
        row = self.conn.execute("SELECT digest FROM result_outbox WHERE race_id=%s", (race_id,)).fetchone()
        return row[0] if row else None

    def insert_outbox(self, race_id: int, card_id: int, message: str, digest: str) -> bool:
        self._ensure_conn()
        result = self.conn.execute("INSERT INTO result_outbox(race_id,card_id,message,digest,state,queued_at) VALUES(%s,%s,%s,%s,'queued',NOW()) ON CONFLICT (race_id) DO NOTHING RETURNING race_id",
                                   (race_id,card_id,message,digest))
        return result.fetchone() is not None


def make_store(demo: bool):
    url = os.getenv("DATABASE_URL", "").strip()
    if demo:
        return SQLiteStore(Path(os.getenv("MON_DB_PATH", "monterrico-demo.sqlite3")))
    if not url:
        raise SystemExit("Para --live define DATABASE_URL de Supabase (no uses SQLite efímero en Railway).")
    return PostgresStore(url)
