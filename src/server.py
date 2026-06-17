from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from mcp.server.fastmcp import FastMCP


# ---------------------------------------------------------------------------
# Constants & types
# ---------------------------------------------------------------------------

ALLOWED_ROLES = {"system", "user", "assistant", "tool"}
Agent = Literal["claude", "codex"]
Transport = Literal["stdio", "sse", "streamable-http"]

_BLOCKED_EXTRA_ARGS: frozenset[str] = frozenset({
    "-p", "--print",
    "--system",
    "--allowedTools", "--allowed-tools",
    "--dangerously-skip-permissions",
})

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_connection: sqlite3.Connection | None = None
_store: "SessionStore | None" = None
_shutdown_timer: threading.Timer | None = None
_shutdown_signal_count = 0


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return repr(value)


def _truncate(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n...<truncated {len(text) - limit} chars>"


def _log(event: str, **fields: Any) -> None:
    payload = " ".join(f"{k}={_safe_json(v)}" for k, v in fields.items())
    print(f"[{_now_iso()}] [{event}] {payload}".rstrip(), file=sys.stderr, flush=True)


def _log_tool_call(name: str, **arguments: Any) -> None:
    _log("tool.request", tool=name, **arguments)


def _log_tool_result(name: str, result: Any) -> None:
    _log("tool.response", tool=name, result=_truncate(_safe_json(result)))


# ---------------------------------------------------------------------------
# Platform / process guards
# ---------------------------------------------------------------------------

def _is_benign_disconnect_exception(exc: BaseException | None) -> bool:
    if exc is None:
        return False
    if isinstance(exc, ConnectionResetError):
        return getattr(exc, "winerror", None) == 10054
    if isinstance(exc, BrokenPipeError):
        return True
    return False


def _install_asyncio_exception_filter(loop: asyncio.AbstractEventLoop) -> None:
    default_handler = loop.get_exception_handler()

    def handler(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        exc = context.get("exception")
        if _is_benign_disconnect_exception(exc):
            _log("asyncio.disconnect_ignored", message=context.get("message"), error=str(exc))
            return
        if default_handler is not None:
            default_handler(loop, context)
            return
        loop.default_exception_handler(context)

    loop.set_exception_handler(handler)


class _WindowsFilteredEventLoopPolicy(asyncio.WindowsProactorEventLoopPolicy):  # type: ignore[attr-defined]
    def new_event_loop(self) -> asyncio.AbstractEventLoop:
        loop = super().new_event_loop()
        _install_asyncio_exception_filter(loop)
        return loop


def _force_exit(exit_code: int = 130) -> None:
    _log("process.force_exit", exit_code=exit_code)
    global _connection
    if _connection is not None:
        with contextlib.suppress(Exception):
            _connection.close()
        _connection = None
    os._exit(exit_code)


def _start_shutdown_timer(timeout_sec: int) -> None:
    global _shutdown_timer
    if _shutdown_timer is not None:
        return
    _shutdown_timer = threading.Timer(timeout_sec, _force_exit)
    _shutdown_timer.daemon = True
    _shutdown_timer.start()
    _log("shutdown.timer_started", timeout_sec=timeout_sec)


def _cancel_shutdown_timer() -> None:
    global _shutdown_timer
    if _shutdown_timer is not None:
        _shutdown_timer.cancel()
        _shutdown_timer = None
        _log("shutdown.timer_cancelled")


def _install_signal_guards() -> None:
    global _shutdown_signal_count
    timeout_sec = int(os.getenv("ORCH_SHUTDOWN_TIMEOUT_SEC", "5"))

    def handle_shutdown(signum: int, _frame: Any) -> None:
        global _shutdown_signal_count
        _shutdown_signal_count += 1
        _log("signal.received", signum=signum, count=_shutdown_signal_count)
        if _shutdown_signal_count == 1:
            _start_shutdown_timer(timeout_sec)
            raise KeyboardInterrupt()
        _force_exit(130)

    signal.signal(signal.SIGINT, handle_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_shutdown)


def _install_runtime_guards() -> None:
    if os.name == "nt" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(_WindowsFilteredEventLoopPolicy())
        _log("asyncio.policy_installed", policy="WindowsFilteredEventLoopPolicy")
    _install_signal_guards()


def _configure_console_utf8() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
                _log("console.reconfigured", stream=stream_name, encoding="utf-8")
            except Exception as exc:
                _log("console.reconfigure_failed", stream=stream_name, error=str(exc))


# ---------------------------------------------------------------------------
# Message normalization
# ---------------------------------------------------------------------------

def _decode_base64_text(value: str, *, field_name: str) -> str:
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception as exc:
        raise ValueError(f"{field_name}Base64 must be valid UTF-8 base64") from exc


def _resolve_text_value(raw: Any, raw_base64: Any, *, field_name: str) -> str:
    if raw_base64 not in (None, ""):
        return _decode_base64_text(str(raw_base64), field_name=field_name)
    if raw in (None, ""):
        return ""
    return str(raw)


def normalize_messages(messages: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if messages is None:
        return []
    if not isinstance(messages, list):
        raise ValueError("messages must be a list")

    normalized: list[dict[str, str]] = []
    for message in messages:
        role = str(message.get("role", "")).strip()
        content = _resolve_text_value(
            message.get("content"),
            message.get("contentBase64"),
            field_name="messages.content",
        )
        if not role:
            continue
        if role not in ALLOWED_ROLES:
            raise ValueError(f"unsupported role: {role}")
        if content:
            normalized.append({"role": role, "content": content})
    return normalized


# ---------------------------------------------------------------------------
# Prompt compilation
# ---------------------------------------------------------------------------

def _split_messages(
    normalized: list[dict[str, str]],
) -> tuple[list[str], list[dict[str, str]], str]:
    """(system_contents, prior_conversation, last_user_content) 로 분해한다."""
    system_contents = [m["content"] for m in normalized if m["role"] == "system"]
    conversation = [m for m in normalized if m["role"] != "system"]
    last_user = next(
        (m["content"] for m in reversed(conversation) if m["role"] == "user"),
        normalized[-1]["content"],
    )
    return system_contents, conversation[:-1], last_user


def _format_prior(prior: list[dict[str, str]], labels: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for m in prior:
        label = labels.get(m["role"])
        if label:
            lines.append(f"{label} {m['content']}")
    return lines


def compile_claude_parts(messages: list[dict[str, Any]] | None) -> tuple[str | None, str]:
    """claude CLI용 (--system 값, -p 값) 쌍을 반환한다."""
    normalized = normalize_messages(messages)
    if not normalized:
        return None, "사용자 질문에 직접 답하세요."

    system_contents, prior, last_user = _split_messages(normalized)

    system_parts: list[str] = []
    if system_contents:
        system_parts.append("지시사항:")
        system_parts.extend(f"- {c}" for c in system_contents)
        system_parts.append("")
    system_parts.extend([
        "답변 규칙:",
        "- 사용자 질문에 직접 답하세요.",
        "- 인사, 안내, 재질문을 하지 마세요.",
        "- 설명 없이 최종 답변만 출력하세요.",
    ])

    user_parts = _format_prior(prior, {"user": "[이전 질문]", "assistant": "[이전 답변]", "tool": "[도구 결과]"})
    if user_parts:
        user_parts.append("")
    user_parts.append(last_user)

    return "\n".join(system_parts).strip() or None, "\n".join(user_parts).strip()


def compile_codex_prompt(messages: list[dict[str, Any]] | None) -> str:
    """Codex용 프롬프트 — [ROLE] 태그 없이 직접 전달한다."""
    normalized = normalize_messages(messages)
    if not normalized:
        return ""

    system_contents, prior, last_user = _split_messages(normalized)

    parts: list[str] = []
    if system_contents:
        parts.extend(system_contents)
        parts.append("")

    prior_lines = _format_prior(prior, {"user": "이전 요청:", "assistant": "이전 응답:", "tool": "도구 결과:"})
    if prior_lines:
        parts.extend(prior_lines)
        parts.append("")

    parts.append(last_user)
    return "\n".join(parts).strip()


# ---------------------------------------------------------------------------
# CLI execution
# ---------------------------------------------------------------------------

def _resolve_cli_command(name: str) -> str:
    direct = shutil.which(name)
    if direct:
        return direct

    if os.name == "nt":
        for candidate in (f"{name}.cmd", f"{name}.exe", f"{name}.bat"):
            resolved = shutil.which(candidate)
            if resolved:
                return resolved

        appdata = os.getenv("APPDATA")
        if appdata:
            npm_dir = Path(appdata) / "npm"
            for candidate in (name, f"{name}.cmd", f"{name}.exe", f"{name}.bat"):
                candidate_path = npm_dir / candidate
                if candidate_path.exists():
                    return str(candidate_path)

    return name


def _validate_extra_args(args: list[str]) -> None:
    for arg in args:
        if arg.split("=")[0] in _BLOCKED_EXTRA_ARGS:
            raise ValueError(f"extra_args contains blocked argument: {arg!r}")


def _build_command(
    agent: str,
    prompt: str,
    system_prompt: str | None,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
) -> list[str]:
    _validate_extra_args(extra_args)

    if agent == "claude":
        command = [_resolve_cli_command("claude"), "-p", prompt]
        if system_prompt:
            command.extend(["--system", system_prompt])
        if allowed_tools_pattern:
            command.extend(["--allowedTools", allowed_tools_pattern])
        command.extend(extra_args)
        return command

    if agent == "codex":
        command = [_resolve_cli_command("codex"), "exec", "--skip-git-repo-check", prompt]
        command.extend(extra_args)
        return command

    raise ValueError(f"unsupported agent: {agent}")


def run_agent_cli(
    *,
    agent: str,
    prompt: str,
    system_prompt: str | None = None,
    cwd: str | None,
    timeout_ms: int,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
) -> dict[str, Any]:
    command = _build_command(agent, prompt, system_prompt, allowed_tools_pattern, extra_args)
    working_directory = str(Path(cwd).resolve()) if cwd else None
    _log("cli.start", agent=agent, cwd=working_directory, timeout_ms=timeout_ms,
         allowed_tools_pattern=allowed_tools_pattern)

    process = subprocess.Popen(
        command,
        cwd=working_directory,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def consume(stream: Any, buffer: list[str], label: str) -> None:
        try:
            for line in iter(stream.readline, ""):
                buffer.append(line)
                _log(f"cli.{label}", line=line.rstrip("\n"))
        finally:
            stream.close()

    stdout_thread = threading.Thread(target=consume, args=(process.stdout, stdout_chunks, "stdout"), daemon=True)
    stderr_thread = threading.Thread(target=consume, args=(process.stderr, stderr_chunks, "stderr"), daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    timed_out = False
    try:
        exit_code = process.wait(timeout=timeout_ms / 1000)
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        process.terminate()
        try:
            exit_code = process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            exit_code = process.wait(timeout=2)
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        _log("cli.timeout", agent=agent, timeout_ms=timeout_ms)
        raise TimeoutError(f"agent timed out after {timeout_ms} ms") from exc
    finally:
        if not timed_out:
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

    result = {
        "stdout": "".join(stdout_chunks).strip(),
        "stderr": "".join(stderr_chunks).strip(),
        "exitCode": int(exit_code),
    }
    _log("cli.done", agent=agent, exit_code=result["exitCode"],
         stdout=_truncate(result["stdout"]), stderr=_truncate(result["stderr"]))
    return result


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def connect_database(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS sessions (
          id TEXT PRIMARY KEY,
          title TEXT,
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
          sort_order INTEGER,
          is_session INTEGER NOT NULL DEFAULT 1,
          FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_messages_session_id_sort_order ON messages(session_id, sort_order);
    """)
    connection.commit()


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------

class SessionStore:
    def __init__(self, *, connection: sqlite3.Connection, db_path: str, default_timeout_ms: int) -> None:
        self.connection = connection
        self.db_path = db_path
        self.default_timeout_ms = default_timeout_ms
        self.lock = threading.RLock()

    # -- serialization -------------------------------------------------------

    @staticmethod
    def _serialize_session(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "title": row["title"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    @staticmethod
    def _serialize_message(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "sessionId": row["session_id"],
            "role": row["role"],
            "content": row["content"],
            "agent": row["agent"],
            "createdAt": row["created_at"],
            "order": row["sort_order"],
            "isSession": bool(row["is_session"]),
        }

    # -- internal helpers ----------------------------------------------------

    def _session_exists(self, session_id: str) -> bool:
        return self.connection.execute(
            "SELECT id FROM sessions WHERE id = ?", (session_id,)
        ).fetchone() is not None

    def _next_order(self, session_id: str) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM messages WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return int(row["max_order"]) + 1

    def _resolve_session(self, agent: str, session_id: str | None) -> str:
        if not session_id:
            created = self.create_session(title=f"{agent}-{_now_iso()}")
            return created["session"]["id"]
        if self.get_session(session_id) is None:
            raise ValueError(f"session not found: {session_id}")
        return session_id

    def _build_current_messages(
        self,
        prompt: str,
        system_prompt: str | None,
        supplemental: list[dict[str, str]],
        session_id: str,
        use_session: bool,
    ) -> list[dict[str, str]]:
        current: list[dict[str, str]] = []
        if system_prompt:
            should_add = True
            if use_session:
                existing = (self.get_session(session_id) or {}).get("messages", [])
                should_add = not any(
                    m["role"] == "system" and m["content"] == system_prompt and m.get("isSession")
                    for m in existing
                )
            if should_add:
                current.append({"role": "system", "content": system_prompt})
        current.extend(supplemental)
        current.append({"role": "user", "content": prompt})
        return current

    def _get_request_messages(
        self,
        session_id: str,
        current_messages: list[dict[str, str]],
        use_session: bool,
    ) -> list[dict[str, str]]:
        if use_session:
            snapshot = self.get_session(session_id) or {}
            return [
                {"role": m["role"], "content": m["content"]}
                for m in snapshot.get("messages", [])
                if m.get("isSession")
            ]
        return list(current_messages)

    # -- public API ----------------------------------------------------------

    def create_session(
        self,
        title: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        is_session: bool = True,
    ) -> dict[str, Any]:
        session_id = str(uuid4())
        ts = _now_iso()
        normalized = normalize_messages(messages)

        with self.lock:
            self.connection.execute(
                "INSERT INTO sessions (id, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (session_id, title, ts, ts),
            )
            for order, msg in enumerate(normalized, start=1):
                self.connection.execute(
                    "INSERT INTO messages (session_id, role, content, agent, created_at, sort_order, is_session)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (session_id, msg["role"], msg["content"], None, ts, order, 1 if is_session else 0),
                )
            self.connection.commit()

        return self.get_session(session_id)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        if not session_id:
            raise ValueError("sessionId is required")

        with self.lock:
            session_row = self.connection.execute(
                "SELECT id, title, created_at, updated_at FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if session_row is None:
                return None

            messages = [
                self._serialize_message(row)
                for row in self.connection.execute(
                    "SELECT id, session_id, role, content, agent, created_at, sort_order, is_session"
                    " FROM messages WHERE session_id = ? ORDER BY sort_order ASC, id ASC",
                    (session_id,),
                ).fetchall()
            ]
            return {"session": self._serialize_session(session_row), "messages": messages}

    def list_sessions(self, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        with self.lock:
            rows = self.connection.execute(
                "SELECT id, title, created_at, updated_at FROM sessions ORDER BY updated_at DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return [self._serialize_session(row) for row in rows]

    def append_messages(
        self,
        session_id: str,
        messages: list[dict[str, Any]],
        agent: str | None = None,
        is_session: bool = True,
    ) -> dict[str, Any]:
        ts = _now_iso()
        normalized = normalize_messages(messages)

        with self.lock:
            if not self._session_exists(session_id):
                raise ValueError(f"session not found: {session_id}")
            next_order = self._next_order(session_id)
            for msg in normalized:
                self.connection.execute(
                    "INSERT INTO messages (session_id, role, content, agent, created_at, sort_order, is_session)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (session_id, msg["role"], msg["content"], agent, ts, next_order, 1 if is_session else 0),
                )
                next_order += 1
            self.connection.execute(
                "UPDATE sessions SET updated_at = ? WHERE id = ?", (ts, session_id)
            )
            self.connection.commit()

        return self.get_session(session_id)

    def delete_session(self, session_id: str) -> dict[str, Any]:
        with self.lock:
            if not self._session_exists(session_id):
                return {"deleted": False, "sessionId": session_id}
            self.connection.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            self.connection.commit()
        return {"deleted": True, "sessionId": session_id}

    def get_health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "dbPath": self.db_path,
            "defaultTimeoutMs": self.default_timeout_ms,
        }

    # -- agent execution -----------------------------------------------------

    def run_agent(
        self,
        *,
        agent: str,
        prompt: str,
        system_prompt: str | None = None,
        use_session: bool = True,
        session_id: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        allowed_tools_pattern: str | None = "*",
        cwd: str | None = None,
        timeout_ms: int | None = None,
        extra_args: list[str] | None = None,
    ) -> dict[str, Any]:
        if agent not in {"claude", "codex"}:
            raise ValueError("agent must be claude or codex")
        if not prompt:
            raise ValueError("prompt is required")

        active_session_id = self._resolve_session(agent, session_id)
        supplemental = normalize_messages(messages)
        current_messages = self._build_current_messages(
            prompt, system_prompt, supplemental, active_session_id, use_session
        )
        self.append_messages(active_session_id, current_messages, agent=agent, is_session=use_session)

        request_messages = self._get_request_messages(active_session_id, current_messages, use_session)

        if agent == "claude":
            compiled_system, compiled_prompt = compile_claude_parts(request_messages)
        else:
            compiled_system = None
            compiled_prompt = compile_codex_prompt(request_messages)

        resolved_cwd = str(Path(cwd).resolve()) if cwd else str(Path.cwd())
        _log(
            "agent.compiled_prompt",
            agent=agent,
            session_id=active_session_id,
            use_session=use_session,
            cwd=resolved_cwd,
            compiled_system=_truncate(compiled_system or ""),
            compiled_prompt=_truncate(compiled_prompt),
        )

        result = run_agent_cli(
            agent=agent,
            prompt=compiled_prompt,
            system_prompt=compiled_system,
            cwd=resolved_cwd,
            timeout_ms=int(timeout_ms or self.default_timeout_ms),
            allowed_tools_pattern=allowed_tools_pattern,
            extra_args=extra_args or [],
        )
        status = "completed" if result["exitCode"] == 0 else "failed"

        assistant_content = (
            result["stdout"] or "(empty response)"
            if status == "completed"
            else f"[FAILED exit={result['exitCode']}] {result['stderr'] or '(no stderr)'}"
        )
        self.append_messages(
            active_session_id,
            [{"role": "assistant", "content": assistant_content}],
            agent=agent,
            is_session=use_session,
        )
        _log("agent.session_saved", session_id=active_session_id, agent=agent, status=status)

        payload: dict[str, Any] = {
            "runId": str(uuid4()),
            "sessionId": active_session_id,
            "agent": agent,
            "exitCode": result["exitCode"],
            "status": status,
            "stdout": result["stdout"],
            "stderr": result["stderr"],
        }
        if _env_flag("ORCH_DEBUG", True):
            payload["compiledSystem"] = compiled_system
            payload["compiledPrompt"] = compiled_prompt
        _log("agent.result", result=_truncate(_safe_json(payload)))
        return payload


# ---------------------------------------------------------------------------
# Server configuration
# ---------------------------------------------------------------------------

def _get_root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _get_db_path() -> Path:
    return Path(os.getenv("ORCH_DB_PATH", str(_get_root_dir() / "data" / "orchestrator.sqlite"))).resolve()


def _get_transport() -> Transport:
    transport = os.getenv("ORCH_TRANSPORT", "streamable-http").strip().lower()
    if transport not in {"stdio", "sse", "streamable-http"}:
        raise ValueError("ORCH_TRANSPORT must be one of: stdio, sse, streamable-http")
    return transport  # type: ignore[return-value]


def _get_store() -> SessionStore:
    if _store is None:
        raise RuntimeError("session store is not initialized")
    return _store


@contextlib.asynccontextmanager
async def server_lifespan(_: FastMCP) -> AsyncIterator[None]:
    global _connection, _store

    db_path = _get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _connection = connect_database(str(db_path))
    _store = SessionStore(
        connection=_connection,
        db_path=str(db_path),
        default_timeout_ms=int(os.getenv("ORCH_DEFAULT_TIMEOUT_MS", "120000")),
    )
    _log(
        "server.start",
        db_path=str(db_path),
        transport=_get_transport(),
        host=os.getenv("ORCH_HOST", os.getenv("ORCH_SSE_HOST", "127.0.0.1")),
        port=int(os.getenv("ORCH_PORT", os.getenv("ORCH_SSE_PORT", "18282"))),
        debug=_env_flag("ORCH_DEBUG", True),
        log_level=os.getenv("ORCH_LOG_LEVEL", "DEBUG"),
    )
    try:
        yield
    finally:
        _log("server.stop")
        if _connection is not None:
            _connection.close()
        _connection = None
        _store = None


mcp = FastMCP(
    name="nowonbun-orchestration-ai-mcp",
    instructions="Claude/Codex CLI orchestration MCP server",
    host=os.getenv("ORCH_HOST", os.getenv("ORCH_SSE_HOST", "127.0.0.1")),
    port=int(os.getenv("ORCH_PORT", os.getenv("ORCH_SSE_PORT", "18282"))),
    json_response=True,
    debug=_env_flag("ORCH_DEBUG", True),
    log_level=os.getenv("ORCH_LOG_LEVEL", "DEBUG"),
    lifespan=server_lifespan,
)


# ---------------------------------------------------------------------------
# MCP tool handlers
# ---------------------------------------------------------------------------

@mcp.tool(name="orchestrator_health", description="서버 상태와 DB 경로를 반환합니다.")
def orchestrator_health() -> dict[str, Any]:
    _log_tool_call("orchestrator_health")
    result = {
        **_get_store().get_health(),
        "transport": _get_transport(),
        "host": mcp.settings.host,
        "port": mcp.settings.port,
    }
    _log_tool_result("orchestrator_health", result)
    return result


@mcp.tool(name="session_create", description="세션을 생성하고 초기 메시지를 저장합니다.")
def session_create(
    title: str | None = None,
    messages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    _log_tool_call("session_create", title=title)
    result = _get_store().create_session(title=title, messages=messages)
    _log_tool_result("session_create", result)
    return result


@mcp.tool(name="session_get", description="세션과 메시지 목록을 조회합니다.")
def session_get(sessionId: str) -> dict[str, Any] | None:
    _log_tool_call("session_get", sessionId=sessionId)
    result = _get_store().get_session(sessionId)
    _log_tool_result("session_get", result)
    return result


@mcp.tool(name="session_list", description="최근 세션 목록을 조회합니다.")
def session_list(limit: int = 20) -> list[dict[str, Any]]:
    _log_tool_call("session_list", limit=limit)
    result = _get_store().list_sessions(limit=limit)
    _log_tool_result("session_list", result)
    return result


@mcp.tool(name="session_append", description="기존 세션에 메시지를 추가합니다.")
def session_append(
    sessionId: str,
    messages: list[dict[str, Any]],
    agent: str | None = None,
) -> dict[str, Any]:
    _log_tool_call("session_append", sessionId=sessionId, agent=agent)
    result = _get_store().append_messages(session_id=sessionId, messages=messages, agent=agent)
    _log_tool_result("session_append", result)
    return result


@mcp.tool(name="session_delete", description="세션과 연결 메시지를 삭제합니다.")
def session_delete(sessionId: str) -> dict[str, Any]:
    _log_tool_call("session_delete", sessionId=sessionId)
    result = _get_store().delete_session(sessionId)
    _log_tool_result("session_delete", result)
    return result


@mcp.tool(name="agent_run", description="Claude 또는 Codex CLI를 실행하고 필요 시 세션에 저장합니다.")
def agent_run(
    agent: Literal["claude", "codex"],
    prompt: str = "",
    promptBase64: str | None = None,
    systemPrompt: str | None = None,
    systemPromptBase64: str | None = None,
    useSession: bool = True,
    sessionId: str | None = None,
    messages: list[dict[str, Any]] | None = None,
    allowedToolsPattern: str | None = "*",
    cwd: str | None = None,
    timeoutMs: int | None = None,
    extraArgs: list[str] | None = None,
) -> dict[str, Any]:
    resolved_prompt = _resolve_text_value(prompt, promptBase64, field_name="prompt")
    resolved_system = _resolve_text_value(systemPrompt, systemPromptBase64, field_name="systemPrompt") or None
    _log_tool_call(
        "agent_run",
        agent=agent,
        prompt=_truncate(resolved_prompt),
        systemPrompt=_truncate(resolved_system or ""),
        useSession=useSession,
        sessionId=sessionId,
        allowedToolsPattern=allowedToolsPattern,
        cwd=cwd,
        timeoutMs=timeoutMs,
    )
    result = _get_store().run_agent(
        agent=agent,
        prompt=resolved_prompt,
        system_prompt=resolved_system,
        use_session=useSession,
        session_id=sessionId,
        messages=messages,
        allowed_tools_pattern=allowedToolsPattern,
        cwd=cwd,
        timeout_ms=timeoutMs,
        extra_args=extraArgs,
    )
    _log_tool_result("agent_run", result)
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    _configure_console_utf8()
    _install_runtime_guards()
    _log("process.start", file=__file__, transport=_get_transport())
    exit_code = 0
    try:
        mcp.run(transport=_get_transport())
    except KeyboardInterrupt:
        exit_code = 130
        _log("process.keyboard_interrupt")
    finally:
        _cancel_shutdown_timer()
        _log("process.exit", exit_code=exit_code)


if __name__ == "__main__":
    main()
