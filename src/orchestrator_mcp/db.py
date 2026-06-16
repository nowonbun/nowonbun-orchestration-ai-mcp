from __future__ import annotations

import sqlite3


def connect_database(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS sessions (
          id TEXT PRIMARY KEY,
          title TEXT,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          role TEXT NOT NULL,
          content TEXT NOT NULL,
          agent TEXT,
          created_at TEXT NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS runs (
          id TEXT PRIMARY KEY,
          session_id TEXT,
          agent TEXT NOT NULL,
          use_session INTEGER NOT NULL,
          cwd TEXT,
          prompt TEXT NOT NULL,
          system_prompt TEXT,
          request_messages_json TEXT NOT NULL DEFAULT '[]',
          response_text TEXT,
          stderr_text TEXT,
          status TEXT NOT NULL,
          exit_code INTEGER,
          started_at TEXT NOT NULL,
          ended_at TEXT,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_messages_session_id_id ON messages(session_id, id);
        CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);
        """
    )
    _migrate_runs_foreign_key(connection)
    connection.commit()


def _migrate_runs_foreign_key(connection: sqlite3.Connection) -> None:
    foreign_keys = connection.execute("PRAGMA foreign_key_list(runs)").fetchall()
    if any(row["from"] == "session_id" for row in foreign_keys):
        return

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs_new (
          id TEXT PRIMARY KEY,
          session_id TEXT,
          agent TEXT NOT NULL,
          use_session INTEGER NOT NULL,
          cwd TEXT,
          prompt TEXT NOT NULL,
          system_prompt TEXT,
          request_messages_json TEXT NOT NULL DEFAULT '[]',
          response_text TEXT,
          stderr_text TEXT,
          status TEXT NOT NULL,
          exit_code INTEGER,
          started_at TEXT NOT NULL,
          ended_at TEXT,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE SET NULL
        );

        INSERT INTO runs_new (
          id, session_id, agent, use_session, cwd, prompt, system_prompt,
          request_messages_json, response_text, stderr_text, status, exit_code,
          started_at, ended_at, metadata_json
        )
        SELECT
          id,
          session_id,
          agent,
          use_session,
          cwd,
          prompt,
          system_prompt,
          request_messages_json,
          response_text,
          stderr_text,
          status,
          exit_code,
          started_at,
          ended_at,
          metadata_json
        FROM runs;

        DROP TABLE runs;
        ALTER TABLE runs_new RENAME TO runs;

        CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);
        """
    )
