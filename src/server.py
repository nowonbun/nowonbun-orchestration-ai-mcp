from __future__ import annotations

import asyncio
import anyio
import base64
import contextlib
import html
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from mcp.server.fastmcp import FastMCP


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path("D:/work")
DB_PATH = Path("D:/work/security/orchestrator.sqlite")
DEFAULT_TIMEOUT_MS = 3000000
DEFAULT_MCP_PORT = 18282
DEFAULT_WEB_PORT = 18765
DEFAULT_GRACEFUL_SHUTDOWN_SEC = 5
MANUAL_ONLY_MARKER = "- - - - - -"
SECURITY_DENY_ROOTS: tuple[Path, ...] = (
    (BASE_DIR / "security").resolve(),
)
CLAUDE_REVIEW_PROMPT_PREFIX = (
    "Review mode.\n"
    "You must read every referenced local file path before answering.\n"
    "If any referenced file cannot be read, output exactly: BLOCKED|file-unreadable\n"
    "Do not ask follow-up questions.\n"
    "Do not guess.\n"
    "Follow the user's requested output format exactly.\n\n"
)

# ---------------------------------------------------------------------------
# Constants & types
# ---------------------------------------------------------------------------

ALLOWED_ROLES = {"user", "assistant", "tool"}
Agent = Literal["claude", "codex"]
Transport = Literal["streamable-http"]

_BLOCKED_EXTRA_ARGS: frozenset[str] = frozenset({
    "-p", "--print",
    "--allowedTools", "--allowed-tools",
    "--dangerously-skip-permissions",
    "-s", "--sandbox",
    "-a", "--ask-for-approval",
    "--add-dir",
    "--dangerously-bypass-approvals-and-sandbox",
})

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_connection: sqlite3.Connection | None = None
_store: "SessionStore | None" = None
_scheduler: "ScheduleRunner | None" = None
_web_server: ThreadingHTTPServer | None = None
_web_thread: threading.Thread | None = None
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


def _is_manual_only_prompt(prompt: str) -> bool:
    return MANUAL_ONLY_MARKER in prompt


def _is_manual_only_cron(expr: str) -> bool:
    return expr.strip() == MANUAL_ONLY_MARKER


def _looks_like_file_review_failure(stdout: str) -> bool:
    lowered = (stdout or "").lower()
    patterns = (
        "\uc5b4\ub5a4 \ud30c\uc77c",
        "\uc5b4\ub5a4 3\uac1c\uc758 \ud30c\uc77c",
        "\ud30c\uc77c\uc744 \uac80\ud1a0\ud560\uc9c0",
        "\ud30c\uc77c\uc744 \uc9c0\uc815",
        "\uc54c\ub824\uc8fc\uc2dc\uaca0\uc2b5\ub2c8\uae4c",
        "which file",
        "which files",
        "specify the file",
        "specify which file",
    )
    return any(pattern in lowered for pattern in patterns)


def _log(event: str, **fields: Any) -> None:
    payload = " ".join(f"{k}={_safe_json(v)}" for k, v in fields.items())
    print(f"[{_now_iso()}] [{event}] {payload}".rstrip(), file=sys.stderr, flush=True)


def _log_tool_call(name: str, **arguments: Any) -> None:
    _log("tool.request", tool=name, **arguments)


def _log_tool_result(name: str, result: Any) -> None:
    _log("tool.response", tool=name, result=_truncate(_safe_json(result)))


def _json_response(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


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


if hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
    class _WindowsFilteredEventLoopPolicy(asyncio.WindowsProactorEventLoopPolicy):  # type: ignore[attr-defined]
        def new_event_loop(self) -> asyncio.AbstractEventLoop:
            loop = super().new_event_loop()
            _install_asyncio_exception_filter(loop)
            return loop
else:
    _WindowsFilteredEventLoopPolicy = None  # type: ignore[assignment]


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
) -> tuple[list[dict[str, str]], str]:
    """(prior_conversation, last_user_content)로 분해한다."""
    last_user = next(
        (m["content"] for m in reversed(normalized) if m["role"] == "user"),
        normalized[-1]["content"],
    )
    return normalized[:-1], last_user


def _format_prior(prior: list[dict[str, str]], labels: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for m in prior:
        label = labels.get(m["role"])
        if label:
            lines.append(f"{label} {m['content']}")
    return lines


def compile_claude_parts(messages: list[dict[str, Any]] | None) -> str:
    """claude CLI용 -p 값을 반환한다."""
    normalized = normalize_messages(messages)
    if not normalized:
        return ""

    prior, last_user = _split_messages(normalized)

    user_parts = _format_prior(prior, {"user": "[이전 질문]", "assistant": "[이전 답변]", "tool": "[도구 결과]"})
    if user_parts:
        user_parts.append("")
    user_parts.append(last_user)

    return "\n".join(user_parts).strip()


def build_claude_review_prompt(prompt: str) -> str:
    return f"{CLAUDE_REVIEW_PROMPT_PREFIX}{prompt}".strip()


def compile_codex_prompt(messages: list[dict[str, Any]] | None) -> str:
    """Codex용 프롬프트 — [ROLE] 태그 없이 직접 전달한다."""
    normalized = normalize_messages(messages)
    if not normalized:
        return ""

    prior, last_user = _split_messages(normalized)

    parts: list[str] = []
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
    if os.name == "nt" and name == "claude":
        appdata = os.getenv("APPDATA")
        if appdata:
            direct_exe = (
                Path(appdata)
                / "npm"
                / "node_modules"
                / "@anthropic-ai"
                / "claude-code"
                / "bin"
                / "claude.exe"
            )
            if direct_exe.exists():
                return str(direct_exe)

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


def _normalize_injected_file_paths(file_paths: list[str] | None) -> list[str]:
    if file_paths is None:
        return []
    if not isinstance(file_paths, list):
        raise ValueError("filePaths must be a list")
    normalized: list[str] = []
    for raw in file_paths:
        value = str(raw).strip()
        if value:
            normalized.append(value)
    return normalized


def _is_denied_path(path: Path) -> bool:
    for denied_root in SECURITY_DENY_ROOTS:
        try:
            path.relative_to(denied_root)
        except ValueError:
            continue
        return True
    return False


def _resolve_injected_file_path(raw_path: str, *, base_dir: Path) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def _read_files_for_prompt(
    file_paths: list[str],
    *,
    base_dir: Path,
    char_limit_per_file: int = 12000,
) -> str:
    if not file_paths:
        return ""

    sections: list[str] = []
    for raw_path in file_paths:
        resolved = _resolve_injected_file_path(raw_path, base_dir=base_dir)
        if _is_denied_path(resolved):
            raise ValueError("\ubcf4\uc548 \uc815\ucc45\uc5d0 \uc758\ud574 \uc811\uadfc\uc774 \uac70\ubd80\ub418\uc5c8\uc2b5\ub2c8\ub2e4")
        if not resolved.exists():
            raise ValueError(f"filePaths target not found: {resolved}")
        if not resolved.is_file():
            raise ValueError(f"filePaths target is not a file: {resolved}")
        text = resolved.read_text(encoding="utf-8")
        if len(text) > char_limit_per_file:
            text = f"{text[:char_limit_per_file]}\n...<truncated {len(text) - char_limit_per_file} chars>"
        sections.append(f"[FILE] {resolved}\n{text}")
    return "\n\n".join(sections)


def _build_command(
    agent: str,
    prompt: str,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
    *,
    _internal: bool = False,
) -> list[str]:
    if not _internal:
        _validate_extra_args(extra_args)

    if agent == "claude":
        command = [_resolve_cli_command("claude"), "-p", prompt]
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
    cwd: str | None,
    timeout_ms: int,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
    _internal: bool = False,
) -> dict[str, Any]:
    command = _build_command(agent, prompt, allowed_tools_pattern, extra_args, _internal=_internal)
    working_directory = str(Path(cwd).resolve()) if cwd else None
    _log("cli.start", agent=agent, cwd=working_directory, timeout_ms=timeout_ms,
         allowed_tools_pattern=allowed_tools_pattern)
    _log("cli.command", command=command)

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
# Cron schedule helpers
# ---------------------------------------------------------------------------

_CRON_FIELD_RANGES: tuple[tuple[int, int], ...] = (
    (0, 59),   # 분
    (0, 23),   # 시
    (1, 31),   # 일
    (1, 12),   # 월
    (0, 6),    # 요일(월요일=0)
)


def _parse_cron_field(field: str, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            raise ValueError("cron field contains an empty segment")
        if "/" in part:
            base, step_raw = part.split("/", 1)
            if not step_raw.strip():
                raise ValueError(f"cron step value is missing in: {part!r}")
            step = int(step_raw)
            if step <= 0:
                raise ValueError("cron step must be positive")
        else:
            base = part
            step = 1
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_raw, end_raw = base.split("-", 1)
            if not start_raw.strip() or not end_raw.strip():
                raise ValueError(f"cron range is incomplete in: {part!r}")
            start, end = int(start_raw), int(end_raw)
        else:
            if not base.strip():
                raise ValueError(f"cron field value is empty in: {part!r}")
            start = end = int(base)
        if start < minimum or end > maximum or start > end:
            raise ValueError(f"cron value out of range: {part}")
        values.update(range(start, end + 1, step))
    return values


def _parse_cron(expr: str) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
    fields = expr.strip().split()
    if len(fields) != 5:
        raise ValueError("cron expression must have 5 fields")
    parsed = tuple(
        _parse_cron_field(field, minimum, maximum)
        for field, (minimum, maximum) in zip(fields, _CRON_FIELD_RANGES, strict=True)
    )
    return parsed  # type: ignore[return-value]


def _cron_matches(expr: str, candidate_utc: datetime) -> bool:
    if _is_manual_only_cron(expr):
        return False
    minute, hour, day, month, weekday = _parse_cron(expr)
    local = candidate_utc.astimezone()
    return (
        local.minute in minute
        and local.hour in hour
        and local.day in day
        and local.month in month
        and local.weekday() in weekday
    )


def _next_cron_run(expr: str, after_utc: datetime | None = None) -> str | None:
    if _is_manual_only_cron(expr):
        return None
    _parse_cron(expr)
    base = after_utc or datetime.now(timezone.utc)
    candidate = base.replace(second=0, microsecond=0)
    if candidate <= base:
        candidate = candidate.replace(minute=candidate.minute) + _MINUTE
    for _ in range(366 * 24 * 60):
        if _cron_matches(expr, candidate):
            return candidate.isoformat()
        candidate += _MINUTE
    raise ValueError("cron expression has no run within one year")


_MINUTE = timedelta(minutes=1)

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

        CREATE TABLE IF NOT EXISTS scheduled_jobs (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          agent TEXT NOT NULL,
          cron_expr TEXT NOT NULL,
          prompt TEXT NOT NULL,
          skip_permissions INTEGER NOT NULL DEFAULT 0,
          enabled INTEGER NOT NULL DEFAULT 1,
          running INTEGER NOT NULL DEFAULT 0,
          last_run_at TEXT,
          next_run_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scheduled_runs (
          id TEXT PRIMARY KEY,
          job_id TEXT NOT NULL,
          status TEXT NOT NULL,
          exit_code INTEGER,
          stdout TEXT,
          stderr TEXT,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          error TEXT,
          FOREIGN KEY(job_id) REFERENCES scheduled_jobs(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_messages_session_id_sort_order ON messages(session_id, sort_order);
        CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(enabled, running, next_run_at);
        CREATE INDEX IF NOT EXISTS idx_scheduled_runs_job_started ON scheduled_runs(job_id, started_at DESC);
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
        self.active_run_lock = threading.Condition(threading.RLock())
        self.active_run_count = 0

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
        supplemental: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        current: list[dict[str, str]] = []
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

    # -- schedule API --------------------------------------------------------

    @staticmethod
    def _serialize_schedule(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        prompt = row["prompt"]
        return {
            "id": row["id"],
            "name": row["name"],
            "agent": row["agent"],
            "cronExpr": row["cron_expr"],
            "prompt": prompt,
            "manualOnly": _is_manual_only_prompt(str(prompt)),
            "skipPermissions": bool(row["skip_permissions"]),
            "enabled": bool(row["enabled"]),
            "running": bool(row["running"]),
            "lastRunAt": row["last_run_at"],
            "nextRunAt": row["next_run_at"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    @staticmethod
    def _serialize_schedule_run(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "jobId": row["job_id"],
            "status": row["status"],
            "exitCode": row["exit_code"],
            "stdout": row["stdout"],
            "stderr": row["stderr"],
            "startedAt": row["started_at"],
            "finishedAt": row["finished_at"],
            "error": row["error"],
        }

    def create_schedule(
        self,
        *,
        name: str,
        agent: str,
        cron_expr: str,
        prompt: str,
        skip_permissions: bool = False,
        enabled: bool = True,
    ) -> dict[str, Any]:
        if agent not in {"claude", "codex"}:
            raise ValueError("agent must be claude or codex")
        if not name.strip():
            raise ValueError("name is required")
        if not prompt.strip():
            raise ValueError("prompt is required")
        is_manual = _is_manual_only_prompt(prompt) or cron_expr.strip() == MANUAL_ONLY_MARKER
        next_run_at = None if is_manual else _next_cron_run(cron_expr)
        ts = _now_iso()
        job_id = str(uuid4())
        with self.lock:
            self.connection.execute(
                "INSERT INTO scheduled_jobs "
                "(id, name, agent, cron_expr, prompt, skip_permissions, enabled, running, next_run_at, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)",
                (
                    job_id,
                    name.strip(),
                    agent,
                    cron_expr.strip(),
                    prompt,
                    1 if skip_permissions else 0,
                    1 if enabled else 0,
                    next_run_at,
                    ts,
                    ts,
                ),
            )
            self.connection.commit()
        return self.get_schedule(job_id) or {"id": job_id}

    def get_schedule(self, job_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM scheduled_jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return self._serialize_schedule(row)

    def list_schedules(self, include_disabled: bool = True) -> list[dict[str, Any]]:
        where = "" if include_disabled else "WHERE enabled = 1"
        with self.lock:
            rows = self.connection.execute(
                f"SELECT * FROM scheduled_jobs {where} ORDER BY enabled DESC, next_run_at ASC, created_at DESC"
            ).fetchall()
        return [self._serialize_schedule(row) for row in rows if row is not None]

    def update_schedule(
        self,
        *,
        job_id: str,
        name: str | None = None,
        agent: str | None = None,
        cron_expr: str | None = None,
        prompt: str | None = None,
        skip_permissions: bool | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any]:
        current = self.get_schedule(job_id)
        if current is None:
            raise ValueError(f"schedule not found: {job_id}")
        values: dict[str, Any] = {}
        if name is not None:
            if not name.strip():
                raise ValueError("name is required")
            values["name"] = name.strip()
        if agent is not None:
            if agent not in {"claude", "codex"}:
                raise ValueError("agent must be claude or codex")
            values["agent"] = agent
        if cron_expr is not None:
            values["cron_expr"] = cron_expr.strip()
        if prompt is not None:
            if not prompt.strip():
                raise ValueError("prompt is required")
            values["prompt"] = prompt
        if skip_permissions is not None:
            values["skip_permissions"] = 1 if skip_permissions else 0
        effective_prompt = values.get("prompt", current["prompt"])
        effective_cron = values.get("cron_expr", current["cronExpr"])
        is_manual = (
            _is_manual_only_prompt(str(effective_prompt))
            or str(effective_cron).strip() == MANUAL_ONLY_MARKER
        )
        if cron_expr is not None:
            values["next_run_at"] = None if is_manual else _next_cron_run(cron_expr)
        if enabled is not None:
            values["enabled"] = 1 if enabled else 0
            if enabled and "next_run_at" not in values:
                values["next_run_at"] = None if is_manual else _next_cron_run(str(current["cronExpr"]))
        if not values:
            return current
        values["updated_at"] = _now_iso()
        assignments = ", ".join(f"{column} = ?" for column in values)
        with self.lock:
            self.connection.execute(
                f"UPDATE scheduled_jobs SET {assignments} WHERE id = ?",
                (*values.values(), job_id),
            )
            self.connection.commit()
        return self.get_schedule(job_id) or {"id": job_id}

    def delete_schedule(self, job_id: str) -> dict[str, Any]:
        with self.lock:
            row = self.connection.execute("SELECT id FROM scheduled_jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                return {"deleted": False, "jobId": job_id}
            self.connection.execute("DELETE FROM scheduled_jobs WHERE id = ?", (job_id,))
            self.connection.commit()
        return {"deleted": True, "jobId": job_id}

    def list_due_schedule_ids(self, limit: int = 5) -> list[str]:
        now = _now_iso()
        safe_limit = max(1, min(int(limit or 5), 50))
        with self.lock:
            rows = self.connection.execute(
                "SELECT id, prompt, cron_expr FROM scheduled_jobs "
                "WHERE enabled = 1 AND running = 0 AND next_run_at <= ? "
                "ORDER BY next_run_at ASC LIMIT ?",
                (now, safe_limit * 3),
            ).fetchall()
        due_ids: list[str] = []
        for row in rows:
            if _is_manual_only_prompt(str(row["prompt"])) or _is_manual_only_cron(str(row["cron_expr"])):
                continue
            due_ids.append(str(row["id"]))
            if len(due_ids) >= safe_limit:
                break
        return due_ids

    def list_schedule_runs(self, job_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 20), 100))
        with self.lock:
            if job_id:
                rows = self.connection.execute(
                    "SELECT * FROM scheduled_runs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?",
                    (job_id, safe_limit),
                ).fetchall()
            else:
                rows = self.connection.execute(
                    "SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return [self._serialize_schedule_run(row) for row in rows]

    def _claim_schedule_run(self, job_id: str, *, force: bool = False) -> tuple[str, dict[str, Any]]:
        """running=1 을 DB에 동기적으로 기록하고 (run_id, job) 을 반환한다."""
        job = self.get_schedule(job_id)
        if job is None:
            raise ValueError(f"schedule not found: {job_id}")
        if not job["enabled"] and not force:
            raise ValueError(f"schedule is disabled: {job_id}")
        if job.get("manualOnly") and not force:
            raise ValueError(f"schedule is manual-only: {job_id}")
        run_id = str(uuid4())
        started_at = _now_iso()
        with self.lock:
            updated = self.connection.execute(
                "UPDATE scheduled_jobs SET running = 1, updated_at = ? WHERE id = ? AND running = 0",
                (started_at, job_id),
            ).rowcount
            if updated != 1:
                self.connection.rollback()
                raise RuntimeError(f"schedule is already running: {job_id}")
            self.connection.execute(
                "INSERT INTO scheduled_runs (id, job_id, status, started_at) VALUES (?, ?, ?, ?)",
                (run_id, job_id, "running", started_at),
            )
            self.connection.commit()
        return run_id, job

    def _begin_schedule_execution(self) -> None:
        with self.active_run_lock:
            self.active_run_count += 1

    def _end_schedule_execution(self) -> None:
        with self.active_run_lock:
            self.active_run_count = max(0, self.active_run_count - 1)
            self.active_run_lock.notify_all()

    def wait_for_schedule_executions(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        with self.active_run_lock:
            while self.active_run_count > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.active_run_lock.wait(timeout=remaining)
            return True

    def _finalize_schedule_run(
        self,
        *,
        job_id: str,
        run_id: str,
        status: str,
        exit_code: int | None,
        stdout: str,
        stderr: str,
        finished_at: str,
        error: str | None,
        next_run_at: str | None,
    ) -> None:
        try:
            with self.lock:
                self.connection.execute(
                    "UPDATE scheduled_runs SET status = ?, exit_code = ?, stdout = ?, stderr = ?, "
                    "finished_at = ?, error = ? WHERE id = ?",
                    (status, exit_code, stdout, stderr, finished_at, error, run_id),
                )
                self.connection.execute(
                    "UPDATE scheduled_jobs SET running = 0, last_run_at = ?, next_run_at = ?, updated_at = ? WHERE id = ?",
                    (finished_at, next_run_at, finished_at, job_id),
                )
                self.connection.commit()
            return
        except sqlite3.ProgrammingError as exc:
            if "closed database" not in str(exc):
                raise
            _log("schedule.finalize_reopen", job_id=job_id, run_id=run_id, error=str(exc))

        fallback = sqlite3.connect(self.db_path)
        try:
            fallback.execute(
                "UPDATE scheduled_runs SET status = ?, exit_code = ?, stdout = ?, stderr = ?, "
                "finished_at = ?, error = ? WHERE id = ?",
                (status, exit_code, stdout, stderr, finished_at, error, run_id),
            )
            fallback.execute(
                "UPDATE scheduled_jobs SET running = 0, last_run_at = ?, next_run_at = ?, updated_at = ? WHERE id = ?",
                (finished_at, next_run_at, finished_at, job_id),
            )
            fallback.commit()
        finally:
            fallback.close()

    def _execute_schedule_run(self, *, job_id: str, run_id: str, job: dict[str, Any]) -> dict[str, Any]:
        """실제 에이전트 실행 및 결과 DB 저장. _claim_schedule_run 이후에 호출한다."""
        self._begin_schedule_execution()
        status = "failed"
        exit_code: int | None = None
        stdout = ""
        stderr = ""
        error: str | None = None
        try:
            extra_args: list[str] = []
            if job.get("skipPermissions") and job["agent"] == "claude":
                extra_args.append("--dangerously-skip-permissions")
            result = self.run_agent(
                agent=job["agent"],
                prompt=job["prompt"],
                extra_args=extra_args if extra_args else None,
                _internal=True,
            )
            status = str(result["status"])
            exit_code = int(result["exitCode"])
            stdout = str(result.get("stdout") or "")
            stderr = str(result.get("stderr") or "")
            return {"runId": run_id, "jobId": job_id, **result}
        except Exception as exc:
            error = str(exc)
            stderr = error
            _log("schedule.run_failed", job_id=job_id, run_id=run_id, error=error)
            return {"runId": run_id, "jobId": job_id, "status": "failed", "error": error}
        finally:
            finished_at = _now_iso()
            try:
                next_run_at = _next_cron_run(job["cronExpr"])
            except Exception as exc:
                next_run_at = None
                error = error or str(exc)
                status = "failed"
            try:
                self._finalize_schedule_run(
                    job_id=job_id,
                    run_id=run_id,
                    status=status,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    finished_at=finished_at,
                    error=error,
                    next_run_at=next_run_at,
                )
            except Exception as exc:
                _log("schedule.finalize_failed", job_id=job_id, run_id=run_id, error=str(exc))
            finally:
                self._end_schedule_execution()

    def run_schedule(self, job_id: str, *, force: bool = False) -> dict[str, Any]:
        run_id, job = self._claim_schedule_run(job_id, force=force)
        return self._execute_schedule_run(job_id=job_id, run_id=run_id, job=job)

    # -- agent execution -----------------------------------------------------

    def run_agent(
        self,
        *,
        agent: str,
        prompt: str,
        use_session: bool = True,
        session_id: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        file_paths: list[str] | None = None,
        allowed_tools_pattern: str | None = "*",
        cwd: str | None = None,
        timeout_ms: int | None = None,
        extra_args: list[str] | None = None,
        _internal: bool = False,
    ) -> dict[str, Any]:
        if agent not in {"claude", "codex"}:
            raise ValueError("agent must be claude or codex")
        if not prompt:
            raise ValueError("prompt is required")

        active_session_id = self._resolve_session(agent, session_id)
        supplemental = normalize_messages(messages)
        injected_file_paths = _normalize_injected_file_paths(file_paths)
        current_messages = self._build_current_messages(prompt, supplemental)
        self.append_messages(active_session_id, current_messages, agent=agent, is_session=use_session)

        request_messages = self._get_request_messages(active_session_id, current_messages, use_session)
        resolved_cwd = str(Path(cwd).resolve()) if cwd else str(_get_base_dir())

        if agent == "claude":
            compiled_prompt = compile_claude_parts(request_messages)
            if injected_file_paths:
                injected_files = _read_files_for_prompt(injected_file_paths, base_dir=Path(resolved_cwd))
                compiled_prompt = f"{compiled_prompt}\n\n[INJECTED FILE CONTENTS]\n{injected_files}".strip()
                compiled_prompt = build_claude_review_prompt(compiled_prompt)
        else:
            compiled_prompt = compile_codex_prompt(request_messages)

        _log(
            "agent.compiled_prompt",
            agent=agent,
            session_id=active_session_id,
            use_session=use_session,
            cwd=resolved_cwd,
            compiled_prompt=_truncate(compiled_prompt),
        )

        result = run_agent_cli(
            agent=agent,
            prompt=compiled_prompt,
            cwd=resolved_cwd,
            timeout_ms=int(timeout_ms or self.default_timeout_ms),
            allowed_tools_pattern=allowed_tools_pattern,
            extra_args=extra_args or [],
            _internal=_internal,
        )
        status = "completed" if result["exitCode"] == 0 else "failed"
        if agent == "claude" and injected_file_paths and _looks_like_file_review_failure(result["stdout"]):
            status = "failed"
            failure_note = "[LOGICAL REVIEW FAILURE] Claude did not follow injected file review instructions."
            result["stderr"] = f"{result['stderr']}\n{failure_note}".strip()

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
        if injected_file_paths:
            payload["filePaths"] = injected_file_paths
        if _env_flag("ORCH_DEBUG", True):
            payload["compiledPrompt"] = compiled_prompt
        _log("agent.result", result=_truncate(_safe_json(payload)))
        return payload




# ---------------------------------------------------------------------------
# Local scheduler and Web UI
# ---------------------------------------------------------------------------

class ScheduleRunner:
    """로컬 cron 작업을 백그라운드에서 실행하는 실행기."""

    def __init__(self, *, store: SessionStore, interval_seconds: int = 30) -> None:
        self.store = store
        self.interval_seconds = max(5, int(interval_seconds or 30))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="orch-scheduler", daemon=True)

    def start(self) -> None:
        self._thread.start()
        _log("schedule.runner_started", interval_seconds=self.interval_seconds)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        _log("schedule.runner_stopped")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                for job_id in self.store.list_due_schedule_ids():
                    threading.Thread(
                        target=self.store.run_schedule,
                        kwargs={"job_id": job_id},
                        name=f"orch-schedule-{job_id[:8]}",
                        daemon=True,
                    ).start()
            except Exception as exc:
                _log("schedule.loop_error", error=str(exc))
            self._stop.wait(self.interval_seconds)


def _html_page(title: str, body: str) -> bytes:
    return f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="icon" type="image/jpeg" href="data:image/jpeg;base64,/9j/2wCEAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDIBCQkJDAsMGA0NGDIhHCEyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMv/AABEIAEAAQAMBIgACEQEDEQH/xAGiAAABBQEBAQEBAQAAAAAAAAAAAQIDBAUGBwgJCgsQAAIBAwMCBAMFBQQEAAABfQECAwAEEQUSITFBBhNRYQcicRQygZGhCCNCscEVUtHwJDNicoIJChYXGBkaJSYnKCkqNDU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6g4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2drh4uPk5ebn6Onq8fLz9PX29/j5+gEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoLEQACAQIEBAMEBwUEBAABAncAAQIDEQQFITEGEkFRB2FxEyIygQgUQpGhscEJIzNS8BVictEKFiQ04SXxFxgZGiYnKCkqNTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqCg4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2dri4+Tl5ufo6ery8/T19vf4+fr/2gAMAwEAAhEDEQA/APM/tLk5yeeuep/z6VSv2aWFgTnNWQuRSSwbkNcEZWZ6U4to52OEs2K17DR5bo/KppbKx824K9OM13C6xo3hKzZLmF7m8lhVo41HAPqx7DitJ1ndRhqzmjSVm5bHHTaJNC2Ch/Kp7PRpZGxtP5U658XXuoyl/LgjJOdqxZX8K6bwfrdlqd2tjeJHBdtxGwPyyew9DUV516cHKxdFUZSszMXw+4HKH8qadCYH7tepvpKAH5R0qsdHDMflA4rzaeYSkz0XhYJHj6NjAqXcMVUV+Pen7+K7uU5+Y0LFVWUN3rDYrqOtXM95J+7VyOTjgcAfkK2rNs7OcVnwWRF7eLEULCc4JGQB16fjWlD4mZVldI7jw2NIt7fzJEtXiPUtGGAH403xZBoAht9U8PSwboJB54iGNpzkNjtz6cVS0W0edLqJ2AdIg2QuBnPpWpZ6O50u9GoXYMZgcIpQckjjBH4V2uKtczlFtJWPSbG4Go6VaXox++hWTgccjNK0e3Gf0qLw+iw+GdMjySFtY1yf90VPMBg8H86+VdHkm0u56cJ3irnzaHxTxJ8tQEEUAnFe/Y8+5q2koWNSasRqouzKh+/1HvVK2hkmVI4o3d24CoMk/hWkthcxTNDKhjlj6oeqn0PpUxTUtDWLuieG8e21AkTzRZwT5aZbHtxXVaaBfXEESljDI3KOP4e+R9K5m2f58SwlmX8DXoXhOxQ2st/LHiRyVjH91f8A65/zzXbKXLC5inqdXtEcQVcBQMAen+f8+1OeX5MDrVljlFzjkf5/z/kZ84wpx1FeTKlqdEJnBeB/AaapOl5qtvIbcgGGFSoMn+0c9v516k+haRa26WzadC9rGRm2uLdPkyeoAGCM101lFFYxJGihokACsTkqPy6VQ165hkhkRwEmhXcOeqn09q7acb7nLOd2Vml0zTdDkXT7C3toz0jgjCfMe/HevKtQ8OXaTPewgzq5LOQPmGfUV2sDtc28WTxyf1Natla4YHpXdRoxn8RhOrKn8J5pZ6XLMM7ACO5713ugaVcS6WYol3Sw8svqp9P14rqYrWKVdjxRnJHzFBkVu2sMcOQgUD2AFKtT6MmOJ5tEjzyRNg2sjK46huo/z/ntihcN8h55BxXo+t6Qmo2jtGoW5UZRvX2NeZXTHDZHI6iuKcLM6adTmP/Z">
  <style>
    :root {{
      --bg-primary: #0f1923;
      --bg-secondary: #1a2634;
      --bg-card: #1e2d3d;
      --bg-input: #0f1923;
      --border: #2a3a4a;
      --text-primary: #e2e8f0;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
      --accent-blue: #3b82f6;
      --accent-green: #10b981;
      --accent-red: #ef4444;
      --accent-orange: #f59e0b;
      --accent-purple: #8b5cf6;
    }}
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', sans-serif; background: var(--bg-primary); color: var(--text-primary); min-height: 100vh; padding: 24px; }}
    a {{ color: var(--accent-blue); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}

    .header {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; }}
    .header h1 {{ font-size: 1.5rem; font-weight: 700; }}
    .header .subtitle {{ color: var(--text-muted); font-size: 0.85rem; }}

    .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }}
    .stat-card {{ background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
    .stat-card .label {{ color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }}
    .stat-card .value {{ font-size: 1.8rem; font-weight: 700; }}
    .stat-card .value.blue {{ color: var(--accent-blue); }}
    .stat-card .value.green {{ color: var(--accent-green); }}
    .stat-card .value.red {{ color: var(--accent-red); }}
    .stat-card .value.purple {{ color: var(--accent-purple); }}

    .main-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
    .card {{ background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 24px; }}
    .card h2 {{ font-size: 1.1rem; font-weight: 600; margin-bottom: 16px; }}

    .form-group {{ margin-bottom: 16px; }}
    .form-group label {{ display: block; color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 6px; }}
    .form-group input, .form-group textarea, .form-group select {{
      width: 100%; background: var(--bg-input); border: 1px solid var(--border); border-radius: 8px;
      padding: 10px 12px; color: var(--text-primary); font-size: 0.9rem; outline: none; transition: border-color 0.2s;
    }}
    .form-group input:focus, .form-group textarea:focus, .form-group select:focus {{ border-color: var(--accent-blue); }}
    .form-group textarea {{ min-height: 100px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; resize: vertical; }}
    .form-group select {{ appearance: none; cursor: pointer; }}

    .agent-select {{ display: flex; gap: 12px; }}
    .agent-option {{ flex: 1; display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--bg-input); border: 2px solid var(--border); border-radius: 8px; cursor: pointer; transition: border-color 0.2s; }}
    .agent-option:has(input:checked) {{ border-color: var(--accent-blue); }}
    .agent-option input {{ display: none; }}
    .agent-option .agent-icon {{ width: 24px; height: 24px; border-radius: 6px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 0.7rem; }}
    .agent-option .agent-icon.claude {{ background: #d97706; color: #fff; }}
    .agent-option .agent-icon.codex {{ background: #059669; color: #fff; }}
    .agent-option .agent-name {{ font-size: 0.9rem; }}

    .checkbox-group {{ display: flex; align-items: center; gap: 8px; margin-bottom: 16px; }}
    .checkbox-group input[type="checkbox"] {{ width: 16px; height: 16px; accent-color: var(--accent-blue); }}
    .checkbox-group label {{ color: var(--text-secondary); font-size: 0.85rem; }}

    .btn {{ padding: 10px 20px; border: none; border-radius: 8px; font-size: 0.9rem; font-weight: 600; cursor: pointer; transition: opacity 0.2s; }}
    .btn:hover {{ opacity: 0.85; }}
    .btn-primary {{ background: var(--accent-blue); color: #fff; }}
    .btn-sm {{ padding: 6px 12px; font-size: 0.8rem; border-radius: 6px; }}
    .btn-green {{ background: var(--accent-green); color: #fff; }}
    .btn-orange {{ background: var(--accent-orange); color: #fff; }}
    .btn-red {{ background: var(--accent-red); color: #fff; }}
    .btn-ghost {{ background: transparent; border: 1px solid var(--border); color: var(--text-secondary); }}

    .activity-list {{ list-style: none; }}
    .activity-item {{ display: flex; align-items: flex-start; gap: 12px; padding: 12px 0; border-bottom: 1px solid var(--border); }}
    .activity-item:last-child {{ border-bottom: none; }}
    .activity-dot {{ width: 8px; height: 8px; border-radius: 50%; margin-top: 6px; flex-shrink: 0; }}
    .activity-dot.success {{ background: var(--accent-green); }}
    .activity-dot.failed {{ background: var(--accent-red); }}
    .activity-dot.running {{ background: var(--accent-orange); }}
    .activity-info .activity-title {{ font-size: 0.9rem; margin-bottom: 2px; }}
    .activity-info .activity-time {{ font-size: 0.75rem; color: var(--text-muted); }}

    .schedule-list {{ list-style: none; }}
    .schedule-item {{ display: flex; align-items: center; justify-content: space-between; padding: 14px 0; border-bottom: 1px solid var(--border); }}
    .schedule-item:last-child {{ border-bottom: none; }}
    .schedule-meta {{ display: flex; align-items: center; gap: 10px; }}
    .schedule-meta .agent-badge {{ padding: 3px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; }}
    .schedule-meta .agent-badge.claude {{ background: rgba(217,119,6,0.2); color: #f59e0b; }}
    .schedule-meta .agent-badge.codex {{ background: rgba(5,150,105,0.2); color: #10b981; }}
    .schedule-info .schedule-name {{ font-size: 0.9rem; font-weight: 500; }}
    .schedule-info .schedule-cron {{ font-size: 0.75rem; color: var(--text-muted); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    .schedule-info .schedule-prompt {{ font-size: 0.75rem; color: var(--text-secondary); margin-top: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 400px; }}
    .schedule-actions {{ display: flex; gap: 6px; align-items: center; }}
    .schedule-actions form {{ display: inline; }}

    .status-badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }}
    .status-badge.running {{ background: rgba(245,158,11,0.2); color: #f59e0b; }}
    .status-badge.enabled {{ background: rgba(16,185,129,0.2); color: #10b981; }}
    .status-badge.disabled {{ background: rgba(100,116,139,0.2); color: #94a3b8; }}

    .empty {{ color: var(--text-muted); text-align: center; padding: 32px 0; font-size: 0.9rem; }}

    .runs-table {{ width: 100%; border-collapse: collapse; }}
    .runs-table th {{ text-align: left; padding: 10px 12px; color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid var(--border); }}
    .runs-table td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 0.85rem; vertical-align: top; }}
    .runs-table .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.8rem; color: var(--text-secondary); }}

    .modal-overlay {{ display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6); z-index: 1000; align-items: center; justify-content: center; }}
    .modal-overlay.active {{ display: flex; }}
    .modal {{ background: var(--bg-card); border: 1px solid var(--border); border-radius: 16px; padding: 32px; width: 90%; max-width: 520px; max-height: 90vh; overflow-y: auto; }}
    .modal h2 {{ margin-bottom: 20px; }}
    .modal .btn-row {{ display: flex; gap: 12px; margin-top: 20px; }}
    .toast-container {{ position: fixed; top: 20px; right: 20px; z-index: 2000; display: flex; flex-direction: column; gap: 10px; }}
    .toast {{ min-width: 260px; max-width: 420px; padding: 12px 14px; border-radius: 10px; border: 1px solid var(--border); background: var(--bg-card); color: var(--text-primary); box-shadow: 0 10px 30px rgba(0,0,0,0.25); opacity: 0; transform: translateY(-8px); transition: opacity 0.18s ease, transform 0.18s ease; }}
    .toast.show {{ opacity: 1; transform: translateY(0); }}
    .toast.error {{ border-color: rgba(239,68,68,0.4); }}
    .toast.success {{ border-color: rgba(16,185,129,0.35); }}
    .btn[disabled] {{ opacity: 0.65; cursor: wait; }}

    .loading-overlay {{ display: none; position: fixed; inset: 0; background: rgba(15,25,35,0.8); z-index: 3000; align-items: center; justify-content: center; flex-direction: column; gap: 16px; pointer-events: all; }}
    .loading-overlay.active {{ display: flex; }}
    .loading-spinner {{ width: 40px; height: 40px; border: 3px solid rgba(255,255,255,0.12); border-top-color: var(--accent-blue); border-radius: 50%; animation: spin 0.8s linear infinite; }}
    @keyframes spin {{ from {{ transform: rotate(0deg); }} to {{ transform: rotate(360deg); }} }}

    @media (max-width: 900px) {{
      .stats {{ grid-template-columns: repeat(2, 1fr); }}
      .main-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
{body}

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-spinner"></div>
  <div id="loadingText" style="color:var(--text-secondary);font-size:0.9rem;">처리 중...</div>
</div>

<div class="toast-container" id="toastContainer"></div>

<div class="modal-overlay" id="editModal">
  <div class="modal">
    <h2>스케줄 편집</h2>
    <form method="post" action="/schedule/update" id="editForm">
      <input type="hidden" name="jobId" id="edit-jobId">
      <div class="form-group">
        <label>Name</label>
        <input name="name" id="edit-name" required>
      </div>
      <div class="form-group">
        <label>Agent</label>
        <div class="agent-select">
          <label class="agent-option">
            <input type="radio" name="agent" value="claude" id="edit-agent-claude">
            <span class="agent-icon claude">C</span>
            <span class="agent-name">Claude</span>
          </label>
          <label class="agent-option">
            <input type="radio" name="agent" value="codex" id="edit-agent-codex">
            <span class="agent-icon codex">X</span>
            <span class="agent-name">Codex</span>
          </label>
        </div>
      </div>
      <div class="form-group">
        <label>Cron Expression</label>
        <input name="cronExpr" id="edit-cronExpr" required>
      </div>
      <div class="form-group">
        <label>Prompt</label>
        <textarea name="prompt" id="edit-prompt" required></textarea>
      </div>
      <div class="checkbox-group">
        <input type="checkbox" name="skipPermissions" id="edit-skipPermissions">
        <label for="edit-skipPermissions">Skip Permissions (파일 쓰기 허용)</label>
      </div>
      <div class="checkbox-group">
        <input type="checkbox" name="enabled" id="edit-enabled">
        <label for="edit-enabled">활성</label>
      </div>
      <div class="btn-row">
        <button type="submit" class="btn btn-primary">Save Changes</button>
        <button type="button" class="btn btn-ghost" onclick="closeEditModal()">Cancel</button>
      </div>
    </form>
  </div>
</div>

<script>
function showLoading(text) {{
  document.getElementById('loadingText').textContent = text || '처리 중...';
  document.getElementById('loadingOverlay').classList.add('active');
}}
function hideLoading() {{
  document.getElementById('loadingOverlay').classList.remove('active');
}}

function showToast(message, kind = 'error', timeoutMs = 4000) {{
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = 'toast ' + kind;
  toast.textContent = message;
  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  setTimeout(() => {{
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 180);
  }}, timeoutMs);
}}

async function postFormAndReload(form, options = {{}}) {{
  const submitter = options.submitter || form.querySelector('button[type="submit"]');
  const submitterText = submitter ? submitter.textContent : '';
  if (submitter) {{
    submitter.disabled = true;
    if (options.pendingText) submitter.textContent = options.pendingText;
  }}
  showLoading(options.loadingText || options.pendingText || '처리 중...');
  try {{
    const response = await fetch(form.action, {{
      method: 'POST',
      headers: {{
        'X-Requested-With': 'fetch',
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
      }},
      body: new URLSearchParams(new FormData(form)).toString(),
    }});
    const payload = await response.json().catch(() => ({{ ok: false, error: '응답 파싱 실패' }}));
    if (!response.ok || !payload.ok) {{
      throw new Error(payload.error || '요청 실패');
    }}
    window.location.reload();
  }} catch (error) {{
    hideLoading();
    if (submitter) {{
      submitter.disabled = false;
      submitter.textContent = submitterText;
    }}
    if (options.onError) {{
      options.onError(error);
    }} else {{
      showToast(error instanceof Error ? error.message : String(error), 'error');
    }}
  }}
}}

function openEditModal(jobId) {{
  fetch('/api/schedule?jobId=' + encodeURIComponent(jobId))
    .then(r => {{
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    }})
    .then(job => {{
      document.getElementById('edit-jobId').value = job.id;
      document.getElementById('edit-name').value = job.name;
      document.getElementById('edit-cronExpr').value = job.cronExpr;
      document.getElementById('edit-prompt').value = job.prompt;
      document.getElementById('edit-skipPermissions').checked = !!job.skipPermissions;
      document.getElementById('edit-enabled').checked = !!job.enabled;
      if (job.agent === 'codex') {{
        document.getElementById('edit-agent-codex').checked = true;
      }} else {{
        document.getElementById('edit-agent-claude').checked = true;
      }}
      document.getElementById('editModal').classList.add('active');
    }})
    .catch(error => {{
      showToast(error instanceof Error ? error.message : String(error), 'error');
    }});
}}
function closeEditModal() {{
  document.getElementById('editModal').classList.remove('active');
}}
document.getElementById('editModal').addEventListener('click', function(e) {{
  if (e.target === this) closeEditModal();
}});

const createForm = document.querySelector('form[action="/schedule/create"]');
if (createForm) {{
  createForm.addEventListener('submit', function(e) {{
    e.preventDefault();
    postFormAndReload(createForm, {{
      submitter: createForm.querySelector('button[type="submit"]'),
      pendingText: 'Creating...',
      loadingText: '스케줄 생성 중...',
      onError: (error) => showToast(error instanceof Error ? error.message : String(error), 'error'),
    }});
  }});
}}

const editForm = document.getElementById('editForm');
if (editForm) {{
  editForm.addEventListener('submit', function(e) {{
    e.preventDefault();
    postFormAndReload(editForm, {{
      submitter: editForm.querySelector('button[type="submit"]'),
      pendingText: 'Saving...',
      loadingText: '저장 중...',
    }});
  }});
}}

document.querySelectorAll('form[action="/schedule/run"]').forEach(function(form) {{
  form.addEventListener('submit', function(e) {{
    e.preventDefault();
    const btn = form.querySelector('button[type="submit"]');
    if (btn && btn.disabled) return;
    const jobItem = form.closest('[data-job-id]');
    if (btn) {{
      btn.disabled = true;
      btn.style.opacity = '0.5';
      btn.style.cursor = 'not-allowed';
    }}
    showLoading('배치 실행 중... 완료될 때까지 기다려주세요.');
    fetch(form.action, {{
      method: 'POST',
      headers: {{
        'X-Requested-With': 'fetch',
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
      }},
      body: new URLSearchParams(new FormData(form)).toString(),
    }})
      .then(function(r) {{ return r.json(); }})
      .then(function(payload) {{
        if (!payload.ok) {{
          hideLoading();
          if (btn) {{
            btn.disabled = false;
            btn.style.opacity = '';
            btn.style.cursor = '';
          }}
          showToast(payload.error || '실행 실패', 'error');
          return;
        }}
        if (jobItem) {{
          var badge = jobItem.querySelector('[data-status-badge]');
          var runBtn = jobItem.querySelector('[data-run-btn]');
          if (badge) {{ badge.className = 'status-badge running'; badge.textContent = 'running'; }}
          if (runBtn) {{ runBtn.disabled = true; runBtn.style.opacity = '0.5'; runBtn.style.cursor = 'not-allowed'; }}
        }}
        if (window._startJobPolling) window._startJobPolling();
      }})
      .catch(function(err) {{
        hideLoading();
        if (btn) {{
          btn.disabled = false;
          btn.style.opacity = '';
          btn.style.cursor = '';
        }}
        showToast(err.message || '네트워크 오류', 'error');
      }});
  }});
}});

document.querySelectorAll('form[action="/schedule/delete"]').forEach(function(form) {{
  form.addEventListener('submit', function(e) {{
    e.preventDefault();
    if (!confirm('Delete this schedule?')) return;
    postFormAndReload(form, {{
      submitter: form.querySelector('button[type="submit"]'),
      pendingText: 'Deleting...',
      loadingText: '삭제 중...',
    }});
  }});
}});

document.querySelectorAll('form[action="/schedule/toggle"]').forEach(function(form) {{
  form.addEventListener('submit', function(e) {{
    e.preventDefault();
    postFormAndReload(form, {{
      submitter: form.querySelector('button[type="submit"]'),
      loadingText: '상태 변경 중...',
    }});
  }});
}});

(function() {{
  var _pollTimer = null;
  var _runLoadingText = '배치 실행 중... 완료될 때까지 기다려주세요.';

  function _updateJobStatus(job) {{
    var item = document.querySelector('[data-job-id="' + job.id + '"]');
    if (!item) return;
    var badge = item.querySelector('[data-status-badge]');
    var runBtn = item.querySelector('[data-run-btn]');
    var status = job.running ? 'running' : (job.enabled ? 'enabled' : 'disabled');
    if (badge) {{
      badge.className = 'status-badge ' + status;
      badge.textContent = status;
    }}
    if (runBtn) {{
      runBtn.disabled = !!job.running;
      runBtn.style.opacity = job.running ? '0.5' : '';
      runBtn.style.cursor = job.running ? 'not-allowed' : '';
    }}
  }}

  function _pollJobs() {{
    fetch('/api/jobs')
      .then(function(r) {{ return r.json(); }})
      .then(function(data) {{
        var runningCount = 0;
        data.jobs.forEach(function(job) {{
          _updateJobStatus(job);
          if (job.running) runningCount++;
        }});
        var label = document.getElementById('runningLabel');
        var subtitle = document.getElementById('dashboardSubtitle');
        if (runningCount > 0) {{
          showLoading(_runLoadingText);
          if (!label && subtitle) {{
            var span = document.createElement('span');
            span.id = 'runningLabel';
            span.style.color = 'var(--accent-orange)';
            span.textContent = '실행 중 ' + runningCount + '개';
            subtitle.appendChild(document.createTextNode(' — '));
            subtitle.appendChild(span);
          }} else if (label) {{
            label.textContent = '실행 중 ' + runningCount + '개';
          }}
        }} else {{
          hideLoading();
          if (label) {{ label.parentNode.removeChild(label.previousSibling); label.parentNode.removeChild(label); }}
          clearInterval(_pollTimer);
          _pollTimer = null;
        }}
      }})
      .catch(function() {{}});
  }}

  function _startPolling() {{
    if (_pollTimer) return;
    _pollTimer = setInterval(_pollJobs, 4000);
  }}

  if (document.querySelector('[data-status-badge].running')) {{
    showLoading(_runLoadingText);
    _startPolling();
  }}

  window._startJobPolling = _startPolling;
}})();
</script>
</body>
</html>""".encode("utf-8")


def _form_bool(values: dict[str, list[str]], key: str, default: bool = False) -> bool:
    if key not in values:
        return default
    return str(values.get(key, [""])[0]).lower() in {"1", "true", "on", "yes"}


def _form_int(values: dict[str, list[str]], key: str) -> int | None:
    raw = values.get(key, [""])[0].strip()
    return int(raw) if raw else None


class WebUiHandler(BaseHTTPRequestHandler):
    server_version = "OrchestrationWebUI/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        _log("web.access", client=self.client_address[0], message=fmt % args)

    @property
    def store(self) -> SessionStore:
        return _get_store()

    def _send(self, status: HTTPStatus, body: str | bytes, content_type: str = "text/html; charset=utf-8") -> None:
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str = "/") -> None:
        self.send_response(HTTPStatus.SEE_OTHER.value)
        self.send_header("Location", location)
        self.end_headers()

    def _read_form(self) -> dict[str, list[str]]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        return parse_qs(raw, keep_blank_values=True)

    def _prefers_json(self) -> bool:
        requested_with = self.headers.get("X-Requested-With", "")
        accept = self.headers.get("Accept", "")
        return requested_with.lower() == "fetch" or "application/json" in accept.lower()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send(HTTPStatus.OK, json.dumps(self.store.get_health(), ensure_ascii=False), "application/json; charset=utf-8")
            return
        if parsed.path == "/runs":
            query = parse_qs(parsed.query)
            self._render_runs(query.get("jobId", [None])[0])
            return
        if parsed.path == "/api/schedule":
            query = parse_qs(parsed.query)
            job_id = query.get("jobId", [None])[0]
            if job_id:
                job = self.store.get_schedule(job_id)
                if job:
                    self._send(HTTPStatus.OK, json.dumps(job, ensure_ascii=False), "application/json; charset=utf-8")
                    return
            self._send(HTTPStatus.NOT_FOUND, json.dumps({"error": "not found"}), "application/json; charset=utf-8")
            return
        if parsed.path == "/api/jobs":
            jobs = self.store.list_schedules(include_disabled=True)
            slim = [{"id": j["id"], "running": j["running"], "enabled": j["enabled"]} for j in jobs]
            self._send(HTTPStatus.OK, json.dumps({"jobs": slim}, ensure_ascii=False), "application/json; charset=utf-8")
            return
        self._render_index()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        form = self._read_form()
        try:
            if parsed.path == "/schedule/create":
                self.store.create_schedule(
                    name=form.get("name", [""])[0],
                    agent=form.get("agent", ["claude"])[0],
                    cron_expr=form.get("cronExpr", [""])[0],
                    prompt=form.get("prompt", [""])[0],
                    skip_permissions=_form_bool(form, "skipPermissions", False),
                    enabled=_form_bool(form, "enabled", False),
                )
            elif parsed.path == "/schedule/toggle":
                job_id = form.get("jobId", [""])[0]
                enabled = _form_bool(form, "enabled", False)
                self.store.update_schedule(job_id=job_id, enabled=enabled)
            elif parsed.path == "/schedule/delete":
                self.store.delete_schedule(form.get("jobId", [""])[0])
            elif parsed.path == "/schedule/update":
                job_id = form.get("jobId", [""])[0]
                self.store.update_schedule(
                    job_id=job_id,
                    name=form.get("name", [None])[0] or None,
                    agent=form.get("agent", [None])[0] or None,
                    cron_expr=form.get("cronExpr", [None])[0] or None,
                    prompt=form.get("prompt", [None])[0] or None,
                    skip_permissions=_form_bool(form, "skipPermissions", False),
                    enabled=_form_bool(form, "enabled", False),
                )
            elif parsed.path == "/schedule/run":
                job_id = form.get("jobId", [""])[0]
                run_id, job = self.store._claim_schedule_run(job_id, force=True)
                threading.Thread(
                    target=self.store._execute_schedule_run,
                    kwargs={"job_id": job_id, "run_id": run_id, "job": job},
                    name=f"orch-manual-run-{job_id[:8]}",
                    daemon=True,
                ).start()
            else:
                self._send(HTTPStatus.NOT_FOUND, _html_page("Not found", "<h1>Not found</h1>"))
                return
            if self._prefers_json():
                self._send(HTTPStatus.OK, _json_response({"ok": True}), "application/json; charset=utf-8")
            else:
                self._redirect("/")
        except Exception as exc:
            if self._prefers_json():
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    _json_response({"ok": False, "error": str(exc)}),
                    "application/json; charset=utf-8",
                )
            else:
                self._send(HTTPStatus.BAD_REQUEST, _html_page("Error", f"<h1>요청 실패</h1><p class='danger'>{html.escape(str(exc))}</p><p><a href='/'>돌아가기</a></p>"))

    def _render_index(self) -> None:
        jobs = self.store.list_schedules(include_disabled=True)
        runs = self.store.list_schedule_runs(job_id=None, limit=10)

        # 통계 계산
        active_count = sum(1 for j in jobs if j["enabled"])
        running_count = sum(1 for j in jobs if j["running"])
        total_runs = len(runs)
        failed_runs = sum(1 for r in runs if r["status"] == "failed")
        agents_used = len(set(j["agent"] for j in jobs)) if jobs else 0

        # 최근 활동
        activity_items = []
        for run in runs[:5]:
            status_class = "success" if run["status"] == "completed" else ("running" if run["status"] == "running" else "failed")
            job_name = next((j["name"] for j in jobs if j["id"] == run["jobId"]), run["jobId"][:8])
            activity_items.append(f"""
<li class="activity-item">
  <span class="activity-dot {status_class}"></span>
  <div class="activity-info">
    <div class="activity-title">{html.escape(job_name)} — {html.escape(run['status'])}</div>
    <div class="activity-time">{html.escape(str(run['startedAt']))}</div>
  </div>
</li>""")

        # 활성 스케줄 목록
        schedule_items = []
        for job in jobs:
            status = "running" if job["running"] else ("enabled" if job["enabled"] else "disabled")
            toggle_value = "0" if job["enabled"] else "1"
            toggle_btn_class = "btn-orange" if job["enabled"] else "btn-green"
            toggle_label = "Pause" if job["enabled"] else "Enable"
            agent_class = html.escape(job["agent"])
            prompt_preview = _truncate(job["prompt"], 80)
            cron_display = "수동 실행 전용" if _is_manual_only_cron(job["cronExpr"]) else html.escape(job["cronExpr"])
            run_btn_disabled = ' disabled style="opacity:0.5;cursor:not-allowed;"' if job["running"] else ""
            schedule_items.append(f"""
<li class="schedule-item" data-job-id="{html.escape(job['id'])}">
  <div style="display:flex;align-items:center;gap:12px;flex:1;min-width:0;">
    <div class="schedule-meta">
      <span class="agent-badge {agent_class}">{html.escape(job['agent'])}</span>
    </div>
    <div class="schedule-info" style="min-width:0;flex:1;">
      <div class="schedule-name">{html.escape(job['name'])} <span class="status-badge {status}" data-status-badge>{status}</span></div>
      <div class="schedule-cron">{cron_display}</div>
      <div class="schedule-prompt">{html.escape(prompt_preview)}</div>
    </div>
  </div>
  <div class="schedule-actions">
    <form method="post" action="/schedule/run"><input type="hidden" name="jobId" value="{html.escape(job['id'])}"><button class="btn btn-sm btn-green" data-run-btn{run_btn_disabled}>Run Now</button></form>
    <button class="btn btn-sm btn-primary" onclick="openEditModal('{html.escape(job['id'])}')">Edit</button>
    <form method="post" action="/schedule/toggle"><input type="hidden" name="jobId" value="{html.escape(job['id'])}"><input type="hidden" name="enabled" value="{toggle_value}"><button class="btn btn-sm {toggle_btn_class}">{toggle_label}</button></form>
    <form method="post" action="/schedule/delete"><input type="hidden" name="jobId" value="{html.escape(job['id'])}"><button class="btn btn-sm btn-red">Delete</button></form>
    <a href="/runs?jobId={html.escape(job['id'])}" class="btn btn-sm btn-ghost">Runs</a>
  </div>
</li>""")

        body = f"""
<div class="header">
  <div>
    <h1>MCP Orchestration</h1>
    <span class="subtitle" id="dashboardSubtitle">스케줄 관리 대시보드{f' — <span style="color:var(--accent-orange)" id="runningLabel">실행 중 {running_count}개</span>' if running_count > 0 else ''}</span>
  </div>
</div>

<div class="stats">
  <div class="stat-card"><div class="label">Active Schedules</div><div class="value blue">{active_count}</div></div>
  <div class="stat-card"><div class="label">Total Runs (Recent)</div><div class="value green">{total_runs}</div></div>
  <div class="stat-card"><div class="label">Failed Runs</div><div class="value red">{failed_runs}</div></div>
  <div class="stat-card"><div class="label">Agents</div><div class="value purple">{agents_used}</div></div>
</div>

<div class="main-grid">
  <div class="card">
    <h2>Create Schedule</h2>
    <form method="post" action="/schedule/create">
      <div class="form-group">
        <label>Name</label>
        <input name="name" placeholder="스케줄 이름" required>
      </div>
      <div class="form-group">
        <label>Agent</label>
        <div class="agent-select">
          <label class="agent-option">
            <input type="radio" name="agent" value="claude" checked>
            <span class="agent-icon claude">C</span>
            <span class="agent-name">Claude</span>
          </label>
          <label class="agent-option">
            <input type="radio" name="agent" value="codex">
            <span class="agent-icon codex">X</span>
            <span class="agent-name">Codex</span>
          </label>
        </div>
      </div>
      <div class="form-group">
        <label>Cron Expression</label>
        <input name="cronExpr" value="*/10 * * * *" placeholder="*/10 * * * *" required>
      </div>
      <div class="form-group">
        <label>Prompt</label>
        <textarea name="prompt" placeholder="실행할 프롬프트를 입력..." required></textarea>
      </div>
      <div class="checkbox-group">
        <input type="checkbox" name="skipPermissions" id="skip-perm-check" checked>
        <label for="skip-perm-check">Skip Permissions (파일 쓰기 허용)</label>
      </div>
      <div class="checkbox-group">
        <input type="checkbox" name="enabled" id="enabled-check" checked>
        <label for="enabled-check">활성화하여 생성</label>
      </div>
      <button type="submit" class="btn btn-primary">Create Schedule</button>
    </form>
  </div>

  <div style="display:flex;flex-direction:column;gap:24px;">
    <div class="card">
      <h2>Active Schedules</h2>
      {f'<ul class="schedule-list">{"".join(schedule_items)}</ul>' if schedule_items else '<div class="empty">등록된 스케줄 없음</div>'}
    </div>

    <div class="card">
      <h2>Recent Activity</h2>
      {f'<ul class="activity-list">{"".join(activity_items)}</ul>' if activity_items else '<div class="empty">실행 기록 없음</div>'}
      <div style="margin-top:12px;"><a href="/runs">모든 실행 기록 보기 →</a></div>
    </div>
  </div>
</div>
"""
        self._send(HTTPStatus.OK, _html_page("MCP Orchestration", body))

    def _render_runs(self, job_id: str | None) -> None:
        runs = self.store.list_schedule_runs(job_id=job_id, limit=50)
        rows = []
        for run in runs:
            status_class = "success" if run["status"] == "completed" else ("running" if run["status"] == "running" else "failed")
            status_color = "var(--accent-green)" if run["status"] == "completed" else ("var(--accent-orange)" if run["status"] == "running" else "var(--accent-red)")
            rows.append(f"""
<tr>
  <td class="mono">{html.escape(run['id'][:12])}</td>
  <td><span class="status-badge {status_class}">{html.escape(run['status'])}</span></td>
  <td style="color:{status_color}">{html.escape(str(run['exitCode']))}</td>
  <td class="mono">{html.escape(str(run['startedAt']))}</td>
  <td class="mono">{html.escape(str(run['finishedAt'] or '-'))}</td>
  <td class="mono" style="max-width:400px;overflow:hidden;text-overflow:ellipsis;">{html.escape(_truncate(run.get('stderr') or run.get('error') or '-', 300))}</td>
</tr>""")
        filter_label = f' — job: {html.escape(job_id[:12])}' if job_id else ""
        body = f"""
<div class="header">
  <div>
    <h1>실행 기록{filter_label}</h1>
    <span class="subtitle"><a href="/">← 대시보드로 돌아가기</a></span>
  </div>
</div>
<div class="card">
  <table class="runs-table">
    <thead><tr><th>Run ID</th><th>Status</th><th>Exit</th><th>Started</th><th>Finished</th><th>Error/Stderr</th></tr></thead>
    <tbody>{''.join(rows) if rows else '<tr><td colspan="6" class="empty">실행 기록 없음</td></tr>'}</tbody>
  </table>
</div>
"""
        self._send(HTTPStatus.OK, _html_page("실행 기록", body))



def _start_web_ui() -> None:
    global _web_server, _web_thread
    if not _env_flag("ORCH_WEB_ENABLED", True):
        _log("web.disabled")
        return
    host = os.getenv("ORCH_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("ORCH_WEB_PORT", str(DEFAULT_WEB_PORT)))
    try:
        _web_server = ThreadingHTTPServer((host, port), WebUiHandler)
        _web_thread = threading.Thread(target=_web_server.serve_forever, name="orch-web-ui", daemon=True)
        _web_thread.start()
        _log("web.started", host=host, port=port)
    except Exception as exc:
        _log("web.start_failed", host=host, port=port, error=str(exc))
        _web_server = None
        _web_thread = None


def _stop_web_ui() -> None:
    global _web_server, _web_thread
    if _web_server is None:
        return
    _web_server.shutdown()
    _web_server.server_close()
    if _web_thread is not None:
        _web_thread.join(timeout=5)
    _log("web.stopped")
    _web_server = None
    _web_thread = None

# ---------------------------------------------------------------------------
# Server configuration
# ---------------------------------------------------------------------------

def _get_root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _get_base_dir() -> Path:
    """CLI 실행 시 기본 작업 디렉터리를 반환한다."""
    return BASE_DIR


def _get_db_path() -> Path:
    return DB_PATH.resolve()


def _get_transport() -> Transport:
    return "streamable-http"


async def _run_mcp_server_async() -> None:
    import uvicorn

    starlette_app = mcp.streamable_http_app()
    config = uvicorn.Config(
        starlette_app,
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_level=mcp.settings.log_level.lower(),
        timeout_graceful_shutdown=int(
            os.getenv("ORCH_GRACEFUL_SHUTDOWN_SEC", str(DEFAULT_GRACEFUL_SHUTDOWN_SEC))
        ),
    )
    server = uvicorn.Server(config)
    await server.serve()


def _get_store() -> SessionStore:
    if _store is None:
        raise RuntimeError("session store is not initialized")
    return _store


def _mark_running_interrupted(connection: sqlite3.Connection, *, reason: str) -> None:
    now = _now_iso()
    connection.execute(
        "UPDATE scheduled_runs SET status = 'failed', finished_at = ?, error = ? WHERE status = 'running'",
        (now, reason),
    )
    connection.execute("UPDATE scheduled_jobs SET running = 0 WHERE running = 1")
    connection.commit()


def _reset_stuck_running_jobs(connection: sqlite3.Connection) -> None:
    """서버 재시작 시 이전 비정상 종료로 running=1이 남은 잡을 리셋한다."""
    _mark_running_interrupted(connection, reason="서버가 실행 중에 재시작되었습니다")
    _log("startup.reset_stuck_jobs")


def _initialize() -> None:
    """서버 시작 전에 DB, Scheduler, Web UI를 초기화한다."""
    global _connection, _store, _scheduler

    db_path = _get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _connection = connect_database(str(db_path))
    _reset_stuck_running_jobs(_connection)
    _store = SessionStore(
        connection=_connection,
        db_path=str(db_path),
        default_timeout_ms=DEFAULT_TIMEOUT_MS,
    )
    _scheduler = ScheduleRunner(
        store=_store,
        interval_seconds=int(os.getenv("ORCH_SCHEDULER_INTERVAL_SECONDS", "30")),
    )
    _scheduler.start()
    _start_web_ui()
    _log(
        "server.start",
        db_path=str(db_path),
        transport=_get_transport(),
        host=os.getenv("ORCH_HOST", os.getenv("ORCH_SSE_HOST", "127.0.0.1")),
        port=int(os.getenv("ORCH_PORT", os.getenv("ORCH_SSE_PORT", str(DEFAULT_MCP_PORT)))),
        web_enabled=_env_flag("ORCH_WEB_ENABLED", True),
        web_host=os.getenv("ORCH_WEB_HOST", "127.0.0.1"),
        web_port=int(os.getenv("ORCH_WEB_PORT", str(DEFAULT_WEB_PORT))),
        debug=_env_flag("ORCH_DEBUG", True),
        log_level=os.getenv("ORCH_LOG_LEVEL", "DEBUG"),
    )


def _shutdown() -> None:
    """서버 종료 시 리소스를 해제한다."""
    global _connection, _store, _scheduler
    _log("server.stop")
    _stop_web_ui()
    if _scheduler is not None:
        _scheduler.stop()
        _scheduler = None
    if _store is not None:
        grace_seconds = float(os.getenv("ORCH_SCHEDULE_SHUTDOWN_GRACE_SEC", "5"))
        completed = _store.wait_for_schedule_executions(grace_seconds)
        if not completed:
            _log("shutdown.active_schedule_runs_interrupted", grace_seconds=grace_seconds)
    if _connection is not None:
        with contextlib.suppress(Exception):
            _mark_running_interrupted(_connection, reason="서버 종료로 실행이 중단되었습니다")
        _connection.close()
    _connection = None
    _store = None


@contextlib.asynccontextmanager
async def server_lifespan(_: FastMCP) -> AsyncIterator[None]:
    yield


mcp = FastMCP(
    name="nowonbun-orchestration-ai-mcp",
    instructions="Claude/Codex CLI orchestration MCP server",
    host=os.getenv("ORCH_HOST", os.getenv("ORCH_SSE_HOST", "127.0.0.1")),
    port=int(os.getenv("ORCH_PORT", os.getenv("ORCH_SSE_PORT", str(DEFAULT_MCP_PORT)))),
    json_response=True,
    debug=_env_flag("ORCH_DEBUG", True),
    log_level=os.getenv("ORCH_LOG_LEVEL", "DEBUG"),
    lifespan=server_lifespan,
)


# ---------------------------------------------------------------------------
# MCP tool handlers
# ---------------------------------------------------------------------------

@mcp.tool(name="orchestrator_usage", description="이 MCP 서버의 사용 가이드를 반환합니다. AI가 도구를 올바르게 호출하기 위한 절차와 예시를 포함합니다.")
def orchestrator_usage() -> dict[str, Any]:
    """AI 에이전트가 이 MCP 서버의 도구 모음을 올바르게 사용하기 위한 가이드를 반환한다."""
    _log_tool_call("orchestrator_usage")
    guide = {
        "overview": (
            "이 서버는 Claude/Codex CLI를 세션과 함께 실행하는 오케스트레이터입니다."
            "user/assistant 메시지 기록을 세션에 저장하여 문맥을 유지한 대화를 지원합니다."
        ),
        "workflow": [
            "1. orchestrator_health로 서버가 정상인지 확인한다(선택 사항).",
            "2. session_create로 세션을 생성한다(생략 가능 — agent_run이 자동 생성 가능).",
            "3. agent_run으로 Claude/Codex에 질문을 보내고 응답을 받는다.",
            "4. 동일한 sessionId로 agent_run을 반복하면 대화가 이어진다.",
            "5. session_get으로 이전 대화 기록을 확인할 수 있다.",
        ],
        "tools": {
            "orchestrator_health": {
                "purpose": "서버 상태 확인",
                "params": "없음",
            },
            "orchestrator_usage": {
                "purpose": "이 사용 가이드 조회",
                "params": "없음",
            },
            "session_create": {
                "purpose": "새 세션 생성. 초기 메시지도 함께 전달 가능",
                "params": {
                    "title": "(선택 사항) 세션 이름",
                    "messages": "(선택 사항) [{role: 'user'|'assistant', content: '...'}] 형식의 초기 메시지 배열",
                },
            },
            "session_get": {
                "purpose": "세션의 전체 메시지 기록 조회",
                "params": {"sessionId": "(필수) 세션 ID"},
            },
            "session_list": {
                "purpose": "최근 세션 목록 조회",
                "params": {"limit": "(선택 사항) 조회 개수. 기본값 20, 최대 100"},
            },
            "session_append": {
                "purpose": "기존 세션에 메시지를 수동 추가(CLI 실행 없이 기록만 추가할 때)",
                "params": {
                    "sessionId": "(필수) 세션 ID",
                    "messages": "(필수) [{role, content}] 배열",
                },
            },
            "session_delete": {
                "purpose": "세션과 전체 메시지 삭제",
                "params": {"sessionId": "(필수) 세션 ID"},
            },
            "agent_run": {
                "purpose": "Claude 또는 Codex CLI를 실행하고 결과를 세션에 저장",
                "params": {
                    "agent": "(필수) 'claude' 또는 'codex'",
                    "prompt": "(필수) 사용자 질문 텍스트",
                    "promptBase64": "(선택 사항) prompt를 Base64 인코딩으로 전달할 때 사용",
                    "useSession": "(선택 사항) true: 세션 기록 사용. 기본값 true",
                    "sessionId": "(선택 사항) 기존 세션 ID. 생략 시 새로 생성",
                    "messages": "(선택 사항) 추가 컨텍스트 메시지 [{role, content}]",
                    "filePaths": "(선택 사항) 서버가 UTF-8로 직접 읽어 프롬프트에 주입할 로컬 파일 경로 배열",
                    "allowedToolsPattern": "(선택 사항) CLI에 전달할 허용 도구 패턴. 기본값 '*'",
                    "cwd": "(선택 사항) CLI 실행 디렉터리",
                    "timeoutMs": "(선택 사항) 타임아웃 ms. 기본값 120000",
                    "extraArgs": "(선택 사항) CLI에 전달할 추가 인수 문자열 배열",
                    "skipPermissions": "(선택 사항) true: claude에 --dangerously-skip-permissions 전달. stdin 없이 도구 실행 시 권한 오류 방지. 기본값 false",
                },
                "returns": {
                    "sessionId": "사용된 세션 ID",
                    "status": "'completed' 또는 'failed'",
                    "stdout": "CLI 표준 출력(응답 본문)",
                    "stderr": "CLI 표준 에러 출력",
                    "exitCode": "프로세스 종료 코드(0 = 성공)",
                },
                "example": {
                    "call": 'agent_run(agent="claude", prompt="Python으로 피보나치 수열을 작성해 줘")',
                    "continuation": 'agent_run(agent="claude", prompt="재귀 버전으로 바꿔 줘", sessionId="<이전 sessionId>")',
                },
            },
        },
        "notes": [
            "role은 'user'와 'assistant'만 사용할 수 있습니다('system'은 미지원).",
            "세션을 사용하면 이전 user/assistant 메시지가 자동으로 프롬프트에 포함됩니다.",
            "긴 프롬프트는 promptBase64로 Base64 인코딩하여 전달할 수 있습니다.",
        ],
    }
    _log_tool_result("orchestrator_usage", {"keys": list(guide.keys())})
    return guide


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


@mcp.tool(name="session_delete", description="세션과 관련 메시지를 삭제합니다.")
def session_delete(sessionId: str) -> dict[str, Any]:
    _log_tool_call("session_delete", sessionId=sessionId)
    result = _get_store().delete_session(sessionId)
    _log_tool_result("session_delete", result)
    return result



@mcp.tool(name="agent_run", description="Claude 또는 Codex CLI를 실행하고 필요하면 세션에 저장합니다.")
def agent_run(
    agent: Literal["claude", "codex"],
    prompt: str = "",
    promptBase64: str | None = None,
    useSession: bool = True,
    sessionId: str | None = None,
    messages: list[dict[str, Any]] | None = None,
    filePaths: list[str] | None = None,
    allowedToolsPattern: str | None = "*",
    cwd: str | None = None,
    timeoutMs: int | None = None,
    extraArgs: list[str] | None = None,
    skipPermissions: bool = False,
) -> dict[str, Any]:
    resolved_prompt = _resolve_text_value(prompt, promptBase64, field_name="prompt")
    _log_tool_call(
        "agent_run",
        agent=agent,
        prompt=_truncate(resolved_prompt),
        useSession=useSession,
        sessionId=sessionId,
        filePaths=filePaths,
        allowedToolsPattern=allowedToolsPattern,
        cwd=cwd,
        timeoutMs=timeoutMs,
        skipPermissions=skipPermissions,
    )
    user_extra_args: list[str] = list(extraArgs or [])
    _validate_extra_args(user_extra_args)
    resolved_extra: list[str] = list(user_extra_args)
    if skipPermissions and agent == "claude":
        resolved_extra.append("--dangerously-skip-permissions")
    result = _get_store().run_agent(
        agent=agent,
        prompt=resolved_prompt,
        use_session=useSession,
        session_id=sessionId,
        messages=messages,
        file_paths=filePaths,
        allowed_tools_pattern=allowedToolsPattern,
        cwd=cwd,
        timeout_ms=timeoutMs,
        extra_args=resolved_extra,
        _internal=skipPermissions,
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
    _initialize()
    exit_code = 0
    try:
        anyio.run(_run_mcp_server_async)
    except KeyboardInterrupt:
        exit_code = 130
        _log("process.keyboard_interrupt")
    finally:
        _shutdown()
        _cancel_shutdown_timer()
        _log("process.exit", exit_code=exit_code)


if __name__ == "__main__":
    main()
