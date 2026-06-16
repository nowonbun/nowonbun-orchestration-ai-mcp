from __future__ import annotations

import json
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from sqlite3 import Connection
from uuid import uuid4

from .cli_runners import run_agent_cli
from .prompt_compiler import compile_prompt, normalize_messages


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SessionStore:
    def __init__(self, *, connection: Connection, db_path: str, cache_limit: int, default_timeout_ms: int, sse_server) -> None:
        self.connection = connection
        self.db_path = db_path
        self.cache_limit = cache_limit
        self.default_timeout_ms = default_timeout_ms
        self.sse_server = sse_server
        self.cache: OrderedDict[str, dict] = OrderedDict()
        self.lock = threading.RLock()

    def _serialize_session(self, row) -> dict | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "title": row["title"],
            "metadata": json.loads(row["metadata_json"]),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    def _serialize_message(self, row) -> dict:
        return {
            "id": row["id"],
            "sessionId": row["session_id"],
            "role": row["role"],
            "content": row["content"],
            "agent": row["agent"],
            "createdAt": row["created_at"],
            "metadata": json.loads(row["metadata_json"]),
        }

    def _touch_cache(self, session_id: str, value: dict) -> None:
        if session_id in self.cache:
            self.cache.pop(session_id)
        self.cache[session_id] = value
        while len(self.cache) > self.cache_limit:
            self.cache.popitem(last=False)

    def _drop_cache(self, session_id: str) -> None:
        self.cache.pop(session_id, None)

    def create_session(self, title: str | None = None, messages: list[dict] | None = None, metadata: dict | None = None) -> dict:
        session_id = str(uuid4())
        ts = _now_iso()
        metadata = metadata or {}
        normalized_messages = normalize_messages(messages)

        with self.lock:
            self.connection.execute(
                "INSERT INTO sessions (id, title, metadata_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (session_id, title, json.dumps(metadata, ensure_ascii=False), ts, ts),
            )
            for message in normalized_messages:
                self.connection.execute(
                    "INSERT INTO messages (session_id, role, content, agent, created_at, metadata_json) VALUES (?, ?, ?, ?, ?, ?)",
                    (session_id, message["role"], message["content"], None, ts, "{}"),
                )
            self.connection.commit()

        result = self.get_session(session_id)
        self.sse_server.broadcast("session.updated", {"sessionId": session_id, "action": "created"}, session_id=session_id)
        return result

    def get_session(self, session_id: str) -> dict | None:
        if not session_id:
            raise ValueError("sessionId is required")

        with self.lock:
            cached = self.cache.get(session_id)
            if cached:
                self._touch_cache(session_id, cached)
                return cached

            session_row = self.connection.execute(
                "SELECT id, title, metadata_json, created_at, updated_at FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if session_row is None:
                return None
            messages = [
                self._serialize_message(row)
                for row in self.connection.execute(
                    "SELECT id, session_id, role, content, agent, created_at, metadata_json FROM messages WHERE session_id = ? ORDER BY id ASC",
                    (session_id,),
                ).fetchall()
            ]

            result = {"session": self._serialize_session(session_row), "messages": messages}
            self._touch_cache(session_id, result)
            return result

    def list_sessions(self, limit: int = 20) -> list[dict]:
        safe_limit = max(1, min(int(limit or 20), 100))
        with self.lock:
            rows = self.connection.execute(
                "SELECT id, title, metadata_json, created_at, updated_at FROM sessions ORDER BY updated_at DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return [self._serialize_session(row) for row in rows]

    def append_messages(self, session_id: str, messages: list[dict], agent: str | None = None, metadata: dict | None = None) -> dict:
        existing = self.get_session(session_id)
        if existing is None:
            raise ValueError(f"session not found: {session_id}")

        ts = _now_iso()
        metadata = metadata or {}
        normalized_messages = normalize_messages(messages)

        with self.lock:
            for message in normalized_messages:
                self.connection.execute(
                    "INSERT INTO messages (session_id, role, content, agent, created_at, metadata_json) VALUES (?, ?, ?, ?, ?, ?)",
                    (session_id, message["role"], message["content"], agent, ts, json.dumps(metadata, ensure_ascii=False)),
                )
            self.connection.execute(
                "UPDATE sessions SET updated_at = ? WHERE id = ?",
                (ts, session_id),
            )
            self.connection.commit()

        with self.lock:
            self._drop_cache(session_id)
        result = self.get_session(session_id)
        self.sse_server.broadcast(
            "session.updated",
            {"sessionId": session_id, "action": "appended", "count": len(normalized_messages)},
            session_id=session_id,
        )
        return result

    def delete_session(self, session_id: str) -> dict:
        existing = self.get_session(session_id)
        if existing is None:
            return {"deleted": False, "sessionId": session_id}

        with self.lock:
            self.connection.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            self.connection.commit()

        with self.lock:
            self._drop_cache(session_id)
        self.sse_server.broadcast("session.updated", {"sessionId": session_id, "action": "deleted"}, session_id=session_id)
        return {"deleted": True, "sessionId": session_id}

    def get_health(self) -> dict:
        return {
            "ok": True,
            "dbPath": self.db_path,
            "cachedSessions": len(self.cache),
            "cacheLimit": self.cache_limit,
            "defaultTimeoutMs": self.default_timeout_ms,
        }

    def run_agent(
        self,
        *,
        agent: str,
        prompt: str,
        system_prompt: str | None = None,
        use_session: bool = True,
        session_id: str | None = None,
        messages: list[dict] | None = None,
        allowed_tools_pattern: str | None = "mcp__*",
        cwd: str | None = None,
        timeout_ms: int | None = None,
        extra_args: list[str] | None = None,
    ) -> dict:
        if agent not in {"claude", "codex"}:
            raise ValueError("agent must be claude or codex")
        if not prompt:
            raise ValueError("prompt is required")

        active_session_id = session_id
        if use_session and not active_session_id:
            created = self.create_session(title=f"{agent}-{_now_iso()}", metadata={"autoCreated": True})
            active_session_id = created["session"]["id"]

        session_snapshot = self.get_session(active_session_id) if use_session and active_session_id else None
        historical_messages = [
            {"role": item["role"], "content": item["content"]}
            for item in (session_snapshot or {}).get("messages", [])
        ]
        transient_messages = normalize_messages(messages)
        request_messages = historical_messages + transient_messages
        compiled_prompt = compile_prompt(system_prompt, request_messages, prompt)

        run_id = str(uuid4())
        started_at = _now_iso()
        resolved_cwd = str(Path(cwd).resolve()) if cwd else str(Path.cwd())

        with self.lock:
            self.connection.execute(
                """
                INSERT INTO runs (
                  id, session_id, agent, use_session, cwd, prompt, system_prompt,
                  request_messages_json, response_text, stderr_text, status, exit_code,
                  started_at, ended_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    active_session_id,
                    agent,
                    1 if use_session else 0,
                    resolved_cwd,
                    prompt,
                    system_prompt,
                    json.dumps(request_messages, ensure_ascii=False),
                    None,
                    None,
                    "running",
                    None,
                    started_at,
                    None,
                    json.dumps({"allowedToolsPattern": allowed_tools_pattern, "extraArgs": extra_args or []}, ensure_ascii=False),
                ),
            )
            self.connection.commit()

        self.sse_server.broadcast(
            "run.started",
            {
                "runId": run_id,
                "sessionId": active_session_id,
                "agent": agent,
                "useSession": use_session,
                "cwd": resolved_cwd,
            },
            session_id=active_session_id,
        )

        try:
            result = run_agent_cli(
                agent=agent,
                prompt=compiled_prompt,
                cwd=resolved_cwd,
                timeout_ms=int(timeout_ms or self.default_timeout_ms),
                allowed_tools_pattern=allowed_tools_pattern,
                extra_args=extra_args or [],
                on_stdout=lambda chunk: self.sse_server.broadcast("run.stdout", {"runId": run_id, "sessionId": active_session_id, "chunk": chunk}, session_id=active_session_id),
                on_stderr=lambda chunk: self.sse_server.broadcast("run.stderr", {"runId": run_id, "sessionId": active_session_id, "chunk": chunk}, session_id=active_session_id),
            )

            status = "completed" if result["exitCode"] == 0 else "failed"
            ended_at = _now_iso()
            with self.lock:
                self.connection.execute(
                    "UPDATE runs SET response_text = ?, stderr_text = ?, status = ?, exit_code = ?, ended_at = ?, metadata_json = ? WHERE id = ?",
                    (
                        result["stdout"],
                        result["stderr"],
                        status,
                        result["exitCode"],
                        ended_at,
                        json.dumps({"allowedToolsPattern": allowed_tools_pattern, "extraArgs": extra_args or [], "compiledPrompt": compiled_prompt}, ensure_ascii=False),
                        run_id,
                    ),
                )
                self.connection.commit()

            if use_session and active_session_id and status == "completed":
                self.append_messages(
                    active_session_id,
                    [
                        {"role": "user", "content": prompt},
                        {"role": "assistant", "content": result["stdout"] or "(empty response)"},
                    ],
                    agent=agent,
                    metadata={"source": "agent_run", "runId": run_id},
                )

            self.sse_server.broadcast(
                f"run.{status}",
                {"runId": run_id, "sessionId": active_session_id, "agent": agent, "exitCode": result["exitCode"]},
                session_id=active_session_id,
            )
            return {
                "runId": run_id,
                "sessionId": active_session_id,
                "agent": agent,
                "exitCode": result["exitCode"],
                "status": status,
                "stdout": result["stdout"],
                "stderr": result["stderr"],
                "compiledPrompt": compiled_prompt,
            }
        except Exception as exc:
            ended_at = _now_iso()
            with self.lock:
                self.connection.execute(
                    "UPDATE runs SET stderr_text = ?, status = ?, exit_code = ?, ended_at = ?, metadata_json = ? WHERE id = ?",
                    (
                        str(exc),
                        "failed",
                        -1,
                        ended_at,
                        json.dumps({"allowedToolsPattern": allowed_tools_pattern, "extraArgs": extra_args or [], "compiledPrompt": compiled_prompt}, ensure_ascii=False),
                        run_id,
                    ),
                )
                self.connection.commit()
            self.sse_server.broadcast(
                "run.failed",
                {"runId": run_id, "sessionId": active_session_id, "agent": agent, "error": str(exc)},
                session_id=active_session_id,
            )
            raise
