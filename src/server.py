import asyncio
import anyio
import base64
import contextlib
import functools
import hashlib
import hmac
import html
import json
import os
import secrets
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 이하 호환 분기
    tomllib = None  # type: ignore[assignment]
from collections import OrderedDict
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from mcp.server.fastmcp import Context, FastMCP


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path("D:/work")
DB_PATH = Path("D:/work/security/orchestrator.sqlite")
DEFAULT_LOG_DIR = Path("D:/work/nowonbun-orchestration-ai-mcp")
DEFAULT_TIMEOUT_MS = 300000  # 기본 하드 타임아웃(300초)
DEFAULT_IDLE_TIMEOUT_SEC = 300  # 출력이 없을 때의 idle 타임아웃(초)
DEFAULT_ALIVE_LOG_INTERVAL_SEC = 30  # 실행 중 상태 로그 출력 간격(초)
AGENT_RUN_PROGRESS_INTERVAL_SEC = 30  # MCP client timeout 防止用 progress heartbeat 間隔（秒）
COMPLETED_RUN_CACHE_SIZE = 100  # 완료된 agent_run 결과의 메모리 보관 상한
COMPLETED_RUN_TTL_SEC = 300  # 완료된 agent_run 결과의 보관 초 수
DEFAULT_GRACEFUL_SHUTDOWN_SEC = 5

# export를 사용하지 않는 로컬 운영용 기본값입니다.
# 환경 변수가 설정되어 있으면 환경 변수를 우선하고, 미설정일 때만 아래 기본값을 사용합니다.
#
# 사용 방법:
# - 이 블록은 batch 실행이나 상주 서버 실행 때 매번 export 하고 싶지 않을 경우 수정합니다.
# - 기본값을 변경한 뒤에는 실행 중인 mcp-orchestration-ai 서버를 재시작합니다.
# - MCP tool 허용·승인 설정은 DEFAULT_TOOL_APPROVALS에 모읍니다.
# - Claude 허용 tool은 Claude CLI의 tool 이름으로 나열합니다.
# - Codex 승인 tool은 "server.tool" 형식으로 read/범용 tool과 write tool을 분리해 나열합니다.
# - Codex에서 전체 tool을 승인할 경우 allow_broad_patterns=True 상태에서 "*" 또는 "server.*"를 사용합니다.
# - "*"는 Codex config.toml에 정의된 MCP server 목록에서 "server.*"로 확장됩니다.
# - 외부 공유 상태를 변경하는 write tool은 write 전용 목록으로 분리합니다.
# - approve_mcp_writes=True 일 때만 활성화합니다.
# - "*", "mcp__*", "server.*" 같은 광범위 허용은 allow_broad_patterns=True일 때만 허용합니다.
# - 일반적으로는 개별 tool 이름 나열을 우선합니다.
# - skipPermissions는 UI/API의 명시 지정으로만 사용하며 Claude의 로컬 권한 확인을 우회합니다.
# - 비대화형 batch에서는 필수 값이 부족하거나 모호하면 실행하지 않고 blocked 처리합니다.
#
# 예시:
# - Claude에서 read 계열 MCP tool만 허용:
#   DEFAULT_TOOL_APPROVALS["claude_allowed_tools"] = ("mcp__my_server__read_item",)
# - Codex에서 read/범용 MCP tool을 사전 승인:
#   DEFAULT_TOOL_APPROVALS["codex_approved_tools"] = ("my-server.read_item",)
# - Codex에서 write 계열 MCP tool을 명시 opt-in으로 사전 승인:
#   DEFAULT_TOOL_APPROVALS["codex_approved_write_tools"] = ("my-server.create_item",)
#   DEFAULT_TOOL_APPROVALS["approve_mcp_writes"] = True
# - Codex에서 설정된 MCP server의 전체 tool을 사전 승인:
#   DEFAULT_TOOL_APPROVALS["codex_approved_tools"] = ("*",)
#   DEFAULT_TOOL_APPROVALS["codex_approved_write_tools"] = ("*",)
#   DEFAULT_TOOL_APPROVALS["allow_broad_patterns"] = True
#   DEFAULT_TOOL_APPROVALS["approve_mcp_writes"] = True
# - batch 전용 환경에서 Claude 권한 확인을 생략할 경우 schedule의 Skip Permissions를 활성화합니다.
# - 광범위 pattern을 꼭 사용해야 하는 경우:
#   DEFAULT_TOOL_APPROVALS["allow_broad_patterns"] = True
#   DEFAULT_TOOL_APPROVALS["claude_allowed_tools"] = ("mcp__my_server__*",)
# - 어느 예시든 변경 후에는 mcp-orchestration-ai 서버 재시작이 필요합니다.
DEFAULT_TOOL_APPROVALS: dict[str, Any] = {
    "claude_allowed_tools": ("*",),
    "codex_approved_tools": ("*",),
    "codex_approved_write_tools": ("*",),
    "allow_broad_patterns": True,
    "approve_mcp_writes": True,
}
LOG_FILE_TIMEZONE = timezone(timedelta(hours=9))
BATCH_PROMPT_PREFIX = """Batch non-interactive mode.
This run cannot receive follow-up answers from the user.
Do not ask confirmation questions.
If the requested MCP write or external write has fully specified target, action, and content, execute it without asking for confirmation.
If target, action, or content is missing or ambiguous, do not ask a question; stop with BLOCKED|ambiguous-batch-write and state the missing fields.
If a required MCP tool is unavailable, stop with BLOCKED|tool-unavailable and state the missing tool.
After any MCP write, report target, action, result, failure reason, and retry necessity.
This prefix is always applied to scheduled batch runs."""

# ---------------------------------------------------------------------------
# Constants & types
# ---------------------------------------------------------------------------

ALLOWED_ROLES = {"user", "assistant", "tool"}
Agent = Literal["claude", "codex"]
Transport = Literal["streamable-http"]
WORKFLOW_ALLOWED_STAGES = {
    "plan",
    "plan-review",
    "execute",
    "result-review",
    "ng-fix",
    "re-review",
    "investigate",
    "review",
    "fix",
}
WORKFLOW_ALLOWED_ROLES = {"main", "sub", "planner", "executor", "reviewer", "agent", "system"}

_BLOCKED_EXTRA_ARGS: frozenset[str] = frozenset({
    "-p", "--print",
    "--allowedTools", "--allowed-tools",
    "--dangerously-skip-permissions",
    "-s", "--sandbox",
    "-a", "--ask-for-approval",
    "--add-dir",
    "--dangerously-bypass-approvals-and-sandbox",
})
_CODEX_SANDBOX_MODES: frozenset[str] = frozenset({"read-only", "workspace-write"})
_CODEX_APPROVAL_POLICIES: frozenset[str] = frozenset({"untrusted", "on-failure", "on-request", "never"})
CODEX_DEFAULT_ADD_DIRS: tuple[Path, ...] = (
    Path("/Users/soonyub.hwang/desk/DailyWork").resolve(),
)
FILEPATH_PROTECTED_ROOTS: tuple[Path, ...] = (
    Path("/Users/soonyub.hwang/desk/security").resolve(),
)

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
_log_file_lock = threading.Lock()
_log_file_warning_emitted = False
_workflow_hash_salt = secrets.token_bytes(32)

# 실행 중 에이전트 상태 추적 레지스트리
_active_runs: dict[str, dict[str, Any]] = {}
_completed_runs: OrderedDict[str, dict[str, Any]] = OrderedDict()
_active_runs_lock = threading.Lock()
_ACTIVE_RUN_OBSERVABILITY_NOTE = (
    "process/stdout/stderr activity only; internal model reasoning is not directly observable"
)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """현재 시각을 UTC ISO 8601 문자열로 반환합니다."""
    return datetime.now(timezone.utc).isoformat()


def _env_flag(name: str, default: bool) -> bool:
    """환경 변수를 불리언으로 해석하고 기본값을 반영합니다."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _safe_json(value: Any) -> str:
    """임의 값을 안전한 JSON 문자열로 변환합니다."""
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return repr(value)


def _json_dumps_for_db(value: Any) -> str | None:
    """DB 저장용 JSON 문자열로 변환한다. None은 NULL로 유지한다."""
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_loads_from_db(value: str | None) -> Any:
    """DB에서 읽은 JSON 문자열을 파이썬 값으로 되돌린다."""
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def _workflow_prompt_hash(prompt: str) -> str:
    """원문 프롬프트 저장 대신 salt가 적용된 해시만 생성한다."""
    return hmac.new(_workflow_hash_salt, prompt.encode("utf-8"), hashlib.sha256).hexdigest()


def _normalize_prompt_hash(value: Any) -> str | None:
    """외부 입력 promptHash를 64자 hex 형식으로만 허용한다."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if len(text) == 64 and all(char in "0123456789abcdef" for char in text):
        return text
    raise ValueError("promptHash must be a 64-character hex digest")


def _normalize_workflow_stage(stage: Any) -> str:
    """workflow stage 값을 검증하고 정규화한다."""
    value = str(stage or "").strip()
    if not value:
        raise ValueError("workflow stage is required")
    if value not in WORKFLOW_ALLOWED_STAGES:
        raise ValueError(f"unsupported workflow stage: {value}")
    return value


def _normalize_workflow_role(role: Any) -> str:
    """workflow role 값을 검증하고 정규화한다."""
    value = str(role or "agent").strip()
    if value not in WORKFLOW_ALLOWED_ROLES:
        raise ValueError(f"unsupported workflow role: {value}")
    return value


def _limit_text(value: Any, max_chars: int = 500) -> str | None:
    """로그/DB 저장용 짧은 텍스트로 제한한다."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def _json_response(payload: dict[str, Any]) -> bytes:
    """사전 payload를 UTF-8 JSON 응답 본문으로 변환합니다."""
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _truncate(text: str, limit: int = 2000) -> str:
    """긴 문자열을 로그 표시용 길이로 자릅니다."""
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n...<truncated {len(text) - limit} chars>"


def _is_mcp_tool_cancelled(stdout: str, stderr: str) -> bool:
    """MCP 도구 호출이 취소되었는지 판단합니다."""
    combined = f"{stdout}\n{stderr}".lower()
    patterns = (
        "user cancelled mcp tool call",
        "cancelled mcp tool call",
        "canceled mcp tool call",
    )
    return any(pattern in combined for pattern in patterns)


def _mcp_tool_cancelled_guidance(agent: str, allowed_tools_pattern: str | None) -> dict[str, Any]:
    """MCP 도구 취소 시 안전한 다음 조치를 반환합니다."""
    return {
        "summary": "MCP tool call was cancelled before completion.",
        "agent": agent,
        "allowedToolsPattern": allowed_tools_pattern,
        "safeNextActions": [
            "의도한 MCP 호출이라면 클라이언트 측에서 MCP tool call을 명시적으로 승인한 뒤 다시 실행하세요.",
            "Claude 실행에서는 allowedToolsPattern 또는 ORCH_CLAUDE_ALLOWED_TOOLS에 대상 MCP 도구 이름이 포함되어 있는지 확인하세요.",
            "외부 공유 상태를 변경하는 MCP 작업은 승인을 우회하지 말고 사용자 확인 후 명시적인 허용 설정으로 실행하세요.",
        ],
    }


def _get_log_dir() -> Path:
    """파일 로그 출력 디렉터리를 반환합니다."""
    return Path(os.getenv("ORCH_LOG_DIR", str(DEFAULT_LOG_DIR))).expanduser().resolve()


def _get_log_file_path(now: datetime) -> Path:
    """일별 로그 파일 경로를 반환합니다."""
    return _get_log_dir() / f"{now.astimezone(LOG_FILE_TIMEZONE).strftime('%Y%m%d')}.log"


def _append_log_file(line: str, now: datetime) -> None:
    """로그 한 줄을 일별 로그 파일에 추가합니다."""
    global _log_file_warning_emitted
    with _log_file_lock:
        try:
            log_dir = _get_log_dir()
            log_dir.mkdir(parents=True, exist_ok=True)
            with _get_log_file_path(now).open("a", encoding="utf-8") as handle:
                handle.write(f"{line}\n")
        except Exception as exc:
            if not _log_file_warning_emitted:
                _log_file_warning_emitted = True
                print(
                    f"[{now.isoformat()}] [log.file_write_failed] error={_safe_json(str(exc))}",
                    file=sys.stderr,
                    flush=True,
                )


_LOG_LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}
_CURRENT_LOG_LEVEL: int = _LOG_LEVEL_ORDER.get(
    os.getenv("ORCH_LOG_LEVEL", "DEBUG").upper(), 0
)


def _log(event: str, *, level: str = "INFO", **fields: Any) -> None:
    """구조화 로그를 stderr와 파일에 기록합니다."""
    if _LOG_LEVEL_ORDER.get(level, 0) < _CURRENT_LOG_LEVEL:
        return
    now = datetime.now(timezone.utc)
    payload = " ".join(f"{k}={_safe_json(v)}" for k, v in fields.items())
    line = f"[{now.isoformat()}] [{level}] [{event}] {payload}".rstrip()
    print(line, file=sys.stderr, flush=True)
    _append_log_file(line, now)


def _log_tool_call(name: str, **arguments: Any) -> None:
    """도구 호출 시작을 로그에 기록합니다."""
    _log("tool.request", tool=name, **arguments)


def _log_tool_result(name: str, result: Any) -> None:
    """도구 호출 결과를 로그에 기록합니다."""
    _log("tool.response", tool=name, result=_truncate(_safe_json(result)))


def _serialize_active_run(entry: dict[str, Any], *, now: float) -> dict[str, Any]:
    """관측 가능한 실행 중 run 상태만 반환합니다."""
    return {
        "runId": entry["runId"],
        "agent": entry["agent"],
        "sessionId": entry.get("sessionId"),
        "pid": entry.get("pid"),
        "cwd": entry.get("cwd"),
        "running": True,
        "startedAt": entry.get("startedAt"),
        "elapsedSec": round(now - float(entry["startMonotonic"]), 1),
        "idleSec": round(now - float(entry["lastActivity"]), 1),
        "stdoutLines": int(entry.get("stdoutLines", 0)),
        "stderrLines": int(entry.get("stderrLines", 0)),
        "timeoutMs": int(entry["timeoutMs"]),
        "idleTimeoutSec": int(entry["idleTimeoutSec"]),
        "aliveLogIntervalSec": int(entry["aliveLogIntervalSec"]),
        "observability": "process_io_only",
        "note": _ACTIVE_RUN_OBSERVABILITY_NOTE,
    }


def _prune_completed_runs_locked(now: float | None = None) -> None:
    """완료된 실행 캐시를 TTL과 최대 개수 기준으로 정리한다."""
    current = time.monotonic() if now is None else now
    expired = [
        run_id
        for run_id, entry in _completed_runs.items()
        if current - float(entry.get("completedMonotonic", current)) > COMPLETED_RUN_TTL_SEC
    ]
    for run_id in expired:
        _completed_runs.pop(run_id, None)
    while len(_completed_runs) > COMPLETED_RUN_CACHE_SIZE:
        _completed_runs.popitem(last=False)


def _sanitize_completed_result(result: dict[str, Any]) -> dict[str, Any]:
    """완료 캐시에 저장할 때 과도한 디버그 필드를 제거한다."""
    sanitized = dict(result)
    sanitized.pop("compiledPrompt", None)
    return sanitized


def _summarize_completed_run(entry: dict[str, Any]) -> dict[str, Any]:
    """최근 완료 실행 목록용 요약 정보만 만든다."""
    result = entry.get("result") or {}
    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    return {
        "runId": entry.get("runId"),
        "agent": result.get("agent"),
        "sessionId": result.get("sessionId"),
        "status": result.get("status") or ("failed" if entry.get("error") else "completed"),
        "exitCode": result.get("exitCode"),
        "completedAt": entry.get("completedAt"),
        "stdoutChars": len(stdout),
        "stderrChars": len(stderr),
        "failureReason": result.get("failureReason"),
        "workflowLogError": result.get("workflowLogError"),
    }


def _remember_completed_run(run_id: str, result: dict[str, Any], error: str | None = None) -> None:
    """완료된 실행 결과를 runId 기준 단기 캐시에 저장한다."""
    if not run_id:
        return
    entry = {
        "runId": run_id,
        "completedAt": _now_iso(),
        "completedMonotonic": time.monotonic(),
        "result": _sanitize_completed_result(result),
        "error": error,
    }
    with _active_runs_lock:
        _completed_runs[run_id] = entry
        _completed_runs.move_to_end(run_id)
        _prune_completed_runs_locked()


def _register_pending_agent_run(
    *,
    run_id: str,
    agent: str,
    session_id: str,
    cwd: str | None,
    timeout_ms: int,
) -> None:
    """백그라운드 시작 직후 조회 가능한 pending 실행 상태를 등록한다."""
    now = time.monotonic()
    idle_timeout_sec = int(os.getenv("ORCH_IDLE_TIMEOUT_SEC", str(DEFAULT_IDLE_TIMEOUT_SEC)))
    alive_log_interval = int(os.getenv("ORCH_ALIVE_LOG_INTERVAL_SEC", str(DEFAULT_ALIVE_LOG_INTERVAL_SEC)))
    working_directory = str(Path(cwd).resolve()) if cwd else str(_get_base_dir())
    with _active_runs_lock:
        _active_runs[run_id] = {
            "runId": run_id,
            "agent": agent,
            "sessionId": session_id,
            "pid": None,
            "cwd": working_directory,
            "startedAt": _now_iso(),
            "startMonotonic": now,
            "timeoutMs": timeout_ms,
            "idleTimeoutSec": idle_timeout_sec,
            "aliveLogIntervalSec": alive_log_interval,
            "stdoutLines": 0,
            "stderrLines": 0,
            "lastActivity": now,
        }


def _forget_active_run(run_id: str) -> None:
    """활성 실행 레지스트리에서 실행 항목을 제거한다."""
    with _active_runs_lock:
        _active_runs.pop(run_id, None)


def _snapshot_active_runs(run_id: str | None = None) -> dict[str, Any]:
    """활성 agent_run 상태를 스냅샷으로 반환합니다."""
    now = time.monotonic()
    with _active_runs_lock:
        _prune_completed_runs_locked(now)
        if run_id is not None:
            entry = _active_runs.get(run_id)
            if entry is None:
                completed = _completed_runs.get(run_id)
                if completed is not None:
                    _completed_runs.move_to_end(run_id)
                    return {
                        "running": False,
                        "completed": True,
                        "runId": run_id,
                        "sessionId": (completed.get("result") or {}).get("sessionId"),
                        "status": (completed.get("result") or {}).get("status") or (
                            "failed" if completed.get("error") else "completed"
                        ),
                        "result": completed.get("result"),
                        "error": completed.get("error"),
                        "completedAt": completed.get("completedAt"),
                        "observability": "completed_result",
                        "note": (
                            "background agent_run has completed; full conversation is also available "
                            "via session_get when sessionId is present"
                        ),
                    }
                return {
                    "running": False,
                    "completed": False,
                    "runId": run_id,
                    "run": None,
                    "observability": "process_io_only",
                    "note": _ACTIVE_RUN_OBSERVABILITY_NOTE,
                    "message": "not running or already completed",
                }
            return {
                "running": True,
                "runId": run_id,
                "run": _serialize_active_run(entry, now=now),
                "observability": "process_io_only",
                "note": _ACTIVE_RUN_OBSERVABILITY_NOTE,
            }

        runs = [_serialize_active_run(entry, now=now) for entry in _active_runs.values()]
        recent_completed = [
            _summarize_completed_run(entry)
            for entry in reversed(_completed_runs.values())
        ]
    return {
        "count": len(runs),
        "runs": runs,
        "recentCompletedCount": len(recent_completed),
        "recentCompleted": recent_completed,
        "observability": "process_io_only",
        "note": _ACTIVE_RUN_OBSERVABILITY_NOTE,
    }


# ---------------------------------------------------------------------------
# Platform / process guards
# ---------------------------------------------------------------------------

def _is_benign_disconnect_exception(exc: BaseException | None) -> bool:
    """무시 가능한 연결 종료 예외인지 판단합니다."""
    if exc is None:
        return False
    if isinstance(exc, ConnectionResetError):
        return getattr(exc, "winerror", None) == 10054
    if isinstance(exc, BrokenPipeError):
        return True
    return False


def _install_asyncio_exception_filter(loop: asyncio.AbstractEventLoop) -> None:
    """asyncio 루프에 연결 종료 예외 필터를 설치합니다."""
    default_handler = loop.get_exception_handler()

    def handler(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        """예외 컨텍스트를 분류해 필요한 경우 기본 핸들러로 위임합니다."""
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
        """연결 종료 예외 필터가 포함된 Windows event loop 정책입니다."""
        def new_event_loop(self) -> asyncio.AbstractEventLoop:
            """새 event loop를 만들고 예외 필터를 적용합니다."""
            loop = super().new_event_loop()
            _install_asyncio_exception_filter(loop)
            return loop
else:
    _WindowsFilteredEventLoopPolicy = None  # type: ignore[assignment]


def _force_exit(exit_code: int = 130) -> None:
    """필요한 후처리 후 프로세스를 강제 종료합니다."""
    _log("process.force_exit", exit_code=exit_code)
    global _connection
    if _connection is not None:
        with contextlib.suppress(Exception):
            _connection.close()
        _connection = None
    os._exit(exit_code)


def _start_shutdown_timer(timeout_sec: int) -> None:
    """graceful shutdown 상한 시간을 감시하는 타이머를 시작합니다."""
    global _shutdown_timer
    if _shutdown_timer is not None:
        return
    _shutdown_timer = threading.Timer(timeout_sec, _force_exit)
    _shutdown_timer.daemon = True
    _shutdown_timer.start()
    _log("shutdown.timer_started", timeout_sec=timeout_sec)


def _cancel_shutdown_timer() -> None:
    """시작된 shutdown 타이머를 중지합니다."""
    global _shutdown_timer
    if _shutdown_timer is not None:
        _shutdown_timer.cancel()
        _shutdown_timer = None
        _log("shutdown.timer_cancelled")


def _install_signal_guards() -> None:
    """shutdown 시그널 처리 핸들러를 설치합니다."""
    global _shutdown_signal_count
    timeout_sec = int(os.getenv("ORCH_SHUTDOWN_TIMEOUT_SEC", "5"))

    def handle_shutdown(signum: int, _frame: Any) -> None:
        """shutdown 신호를 단계적으로 처리합니다."""
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
    """실행 환경에 맞는 런타임 보호 장치를 설치합니다."""
    if os.name == "nt" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(_WindowsFilteredEventLoopPolicy())
        _log("asyncio.policy_installed", policy="WindowsFilteredEventLoopPolicy")
    _install_signal_guards()


def _configure_console_utf8() -> None:
    """표준 입출력 스트림을 UTF-8로 재설정합니다."""
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
    """Base64 문자열을 UTF-8 텍스트로 복호화합니다."""
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception as exc:
        raise ValueError(f"{field_name}Base64 must be valid UTF-8 base64") from exc


def _resolve_text_value(raw: Any, raw_base64: Any, *, field_name: str) -> str:
    """일반 텍스트와 Base64 입력의 우선순위를 정해 반환합니다."""
    if raw_base64 not in (None, ""):
        return _decode_base64_text(str(raw_base64), field_name=field_name)
    if raw in (None, ""):
        return ""
    return str(raw)


def normalize_messages(messages: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    """입력 메시지를 role/content 기준으로 정규화합니다."""
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
    """과거 대화 메시지를 프롬프트용 라벨 텍스트로 정리합니다."""
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


def build_batch_prompt(prompt: str) -> str:
    """비대화형 batch 실행용 프롬프트를 구성합니다."""
    return f"{BATCH_PROMPT_PREFIX}\n\n{prompt}".strip()



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
    """CLI 명령 이름을 실행 가능한 경로로 해석합니다."""
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
    """사용자 지정 extraArgs에 금지된 CLI 인수가 없는지 검증합니다."""
    for arg in args:
        if arg.split("=")[0] in _BLOCKED_EXTRA_ARGS:
            raise ValueError(f"extra_args contains blocked argument: {arg!r}")


def _split_tool_patterns(raw: str | None) -> list[str]:
    """도구 패턴 문자열을 분리합니다."""
    if raw is None:
        return []
    normalized = raw.replace(",", " ")
    return [part.strip() for part in normalized.split() if part.strip()]


def _has_broad_tool_pattern(patterns: list[str]) -> bool:
    """광범위 도구 허용 패턴 포함 여부를 확인합니다."""
    return any(pattern in {"*", "mcp__*"} or pattern.endswith(".*") for pattern in patterns)


def _validate_tool_patterns(patterns: list[str], *, allow_broad_env: str) -> None:
    """허용되지 않은 광범위 도구 패턴을 차단합니다."""
    if _has_broad_tool_pattern(patterns) and not _env_flag(
        allow_broad_env,
        bool(DEFAULT_TOOL_APPROVALS["allow_broad_patterns"]),
    ):
        raise ValueError(f"broad tool pattern requires {allow_broad_env}=true")


def _get_default_claude_allowed_tools() -> list[str]:
    """기본 Claude 허용 도구 목록을 반환합니다."""
    raw = os.getenv("ORCH_CLAUDE_ALLOWED_TOOLS")
    if raw is not None:
        patterns = _split_tool_patterns(raw)
    else:
        patterns = list(DEFAULT_TOOL_APPROVALS["claude_allowed_tools"])
    _validate_tool_patterns(patterns, allow_broad_env="ORCH_ALLOW_BROAD_TOOL_PATTERNS")
    return patterns


def _resolve_claude_allowed_tools(allowed_tools_pattern: str | None) -> str | None:
    """Claude에 전달할 허용 도구 패턴을 정리합니다."""
    requested = _split_tool_patterns(allowed_tools_pattern)
    _validate_tool_patterns(requested, allow_broad_env="ORCH_ALLOW_BROAD_TOOL_PATTERNS")
    defaults = _get_default_claude_allowed_tools()
    merged: list[str] = []
    seen: set[str] = set()
    for pattern in [*requested, *defaults]:
        if pattern in seen:
            continue
        seen.add(pattern)
        merged.append(pattern)
    if not merged:
        return None
    return ",".join(merged)


def _get_codex_sandbox_mode() -> str:
    """Codex sandbox 모드를 반환합니다."""
    value = os.getenv("ORCH_CODEX_SANDBOX", "workspace-write").strip()
    if value not in _CODEX_SANDBOX_MODES:
        raise ValueError(f"unsupported ORCH_CODEX_SANDBOX: {value!r}")
    return value


def _get_codex_approval_policy() -> str:
    """Codex approval policy를 반환합니다."""
    value = os.getenv("ORCH_CODEX_APPROVAL_POLICY", "on-request").strip()
    if value not in _CODEX_APPROVAL_POLICIES:
        raise ValueError(f"unsupported ORCH_CODEX_APPROVAL_POLICY: {value!r}")
    return value


def _get_default_codex_mcp_approved_tools() -> list[str]:
    """기본 Codex MCP 승인 도구 목록을 반환합니다."""
    raw = os.getenv("ORCH_CODEX_MCP_APPROVED_TOOLS")
    if raw is not None:
        return _split_tool_patterns(raw)
    return list(DEFAULT_TOOL_APPROVALS["codex_approved_tools"])


def _get_default_codex_mcp_approved_write_tools() -> list[str]:
    """기본 Codex MCP write 승인 도구 목록을 반환합니다."""
    raw = os.getenv("ORCH_CODEX_MCP_APPROVED_WRITE_TOOLS")
    if raw is not None:
        return _split_tool_patterns(raw)
    return list(DEFAULT_TOOL_APPROVALS["codex_approved_write_tools"])


def _get_codex_config_path() -> Path:
    """Codex 설정 파일 경로를 반환합니다."""
    explicit = os.getenv("ORCH_CODEX_CONFIG_PATH") or os.getenv("CODEX_CONFIG_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()

    codex_home = os.getenv("CODEX_HOME")
    if codex_home:
        return (Path(codex_home).expanduser() / "config.toml").resolve()
    return (Path.home() / ".codex" / "config.toml").resolve()


def _load_codex_mcp_server_names() -> list[str]:
    """Codex 설정에서 MCP 서버 이름 목록을 읽습니다."""
    if tomllib is None:
        raise ValueError("Codex MCP wildcard '*' requires Python 3.11+ tomllib or explicit server.* entries")

    config_path = _get_codex_config_path()
    if not config_path.exists():
        raise ValueError(f"Codex config not found for MCP wildcard expansion: {config_path}")
    if not config_path.is_file():
        raise ValueError(f"Codex config path is not a file: {config_path}")

    with config_path.open("rb") as handle:
        data = tomllib.load(handle)

    mcp_servers = data.get("mcp_servers", {})
    if not isinstance(mcp_servers, dict):
        raise ValueError("Codex config mcp_servers must be a table for wildcard expansion")

    server_names = sorted(str(name) for name, value in mcp_servers.items() if isinstance(value, dict))
    if not server_names:
        raise ValueError("Codex MCP wildcard '*' found no configured mcp_servers")
    return server_names


def _normalize_codex_mcp_tool_refs(tool_refs: list[str] | None) -> list[str]:
    """Codex MCP 도구 참조를 server.tool 형식으로 검증/정규화한다."""
    patterns = [str(tool_ref).strip() for tool_ref in (tool_refs or []) if str(tool_ref).strip()]
    _validate_tool_patterns(patterns, allow_broad_env="ORCH_ALLOW_BROAD_TOOL_PATTERNS")

    normalized: list[str] = []
    seen: set[str] = set()
    for tool_ref in patterns:
        if tool_ref == "*":
            # Codex는 mcp_servers.*.tools.* 를 strict-config로 받지 않으므로,
            # 설정된 server별 server.* 형태로 확장합니다.
            candidates = [f"{server_name}.*" for server_name in _load_codex_mcp_server_names()]
        else:
            candidates = [tool_ref]

        for candidate in candidates:
            if "." not in candidate:
                raise ValueError(f"invalid Codex MCP tool reference: {candidate!r}")
            server_name, tool_name = candidate.split(".", 1)
            if not server_name or not tool_name:
                raise ValueError(f"invalid Codex MCP tool reference: {candidate!r}")
            if candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
    return normalized


def _codex_mcp_writes_enabled(approve_writes: bool) -> bool:
    """Codex MCP write 사전 승인이 활성인지 판단합니다."""
    return approve_writes or _env_flag(
        "ORCH_CODEX_APPROVE_MCP_WRITES",
        bool(DEFAULT_TOOL_APPROVALS["approve_mcp_writes"]),
    )


def _collect_codex_mcp_tool_refs(
    approved_tools: list[str] | None = None,
    approved_write_tools: list[str] | None = None,
    *,
    approve_writes: bool = False,
) -> list[str]:
    """승인 대상 Codex MCP 도구 참조를 수집합니다."""
    tool_refs = [
        *_get_default_codex_mcp_approved_tools(),
        *(approved_tools or []),
    ]
    if _codex_mcp_writes_enabled(approve_writes):
        tool_refs.extend(_get_default_codex_mcp_approved_write_tools())
        tool_refs.extend(approved_write_tools or [])
    elif approved_write_tools:
        raise ValueError("codex MCP write tool approval requires approveCodexMcpWrites=true")
    return tool_refs


def _codex_mcp_tool_ref_to_config_arg(tool_ref: str) -> list[str]:
    """server.tool 형식을 Codex config override 인수로 변환한다."""
    server_name, tool_name = tool_ref.split(".", 1)
    key = f"mcp_servers.{server_name}.tools.{tool_name}.approval_mode"
    return ["-c", f'{key}="approve"']


def _build_codex_mcp_approval_config_args(
    approved_tools: list[str] | None = None,
    approved_write_tools: list[str] | None = None,
    *,
    approve_writes: bool = False,
) -> list[str]:
    """Codex 실행 시 MCP 사전 승인용 config override 인수를 구성한다."""
    tool_refs = _collect_codex_mcp_tool_refs(
        approved_tools, approved_write_tools, approve_writes=approve_writes,
    )
    args: list[str] = []
    for tool_ref in _normalize_codex_mcp_tool_refs(tool_refs):
        args.extend(_codex_mcp_tool_ref_to_config_arg(tool_ref))
    return args


def _get_codex_add_dirs() -> list[str]:
    """Codex에 추가 writable 디렉터리 목록을 반환합니다."""
    add_dirs: list[str] = []
    seen: set[Path] = set()
    for candidate in CODEX_DEFAULT_ADD_DIRS:
        resolved = candidate.resolve()
        if not resolved.exists():
            raise ValueError(f"Codex add-dir target not found: {resolved}")
        if not resolved.is_dir():
            raise ValueError(f"Codex add-dir target is not a directory: {resolved}")
        if resolved not in seen:
            seen.add(resolved)
            add_dirs.append(str(resolved))
    return add_dirs


def _normalize_injected_file_paths(file_paths: list[str] | None) -> list[str]:
    """filePaths 입력을 문자열 배열로 정규화합니다."""
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


def _resolve_injected_file_path(raw_path: str, *, base_dir: Path) -> Path:
    """주입 대상 파일 경로를 절대 경로로 해석합니다."""
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def _is_protected_file_path(path: Path) -> bool:
    """보호 경로 하위인지 확인합니다."""
    for protected_root in FILEPATH_PROTECTED_ROOTS:
        try:
            path.relative_to(protected_root)
        except ValueError:
            continue
        return True
    return False


def _read_files_for_prompt(
    file_paths: list[str],
    *,
    base_dir: Path,
    char_limit_per_file: int = 12000,
) -> str:
    """지정 파일을 읽어 프롬프트 주입용 섹션으로 구성합니다."""
    if not file_paths:
        return ""

    sections: list[str] = []
    for raw_path in file_paths:
        resolved = _resolve_injected_file_path(raw_path, base_dir=base_dir)
        if _is_protected_file_path(resolved):
            raise ValueError("Access denied by security policy.")
        if not resolved.exists():
            raise ValueError(f"filePaths target not found: {resolved}")
        if not resolved.is_file():
            raise ValueError(f"filePaths target is not a file: {resolved}")

        with resolved.open("r", encoding="utf-8") as handle:
            text = handle.read(char_limit_per_file + 1)
        if len(text) > char_limit_per_file:
            text = f"{text[:char_limit_per_file]}\n...<truncated at {char_limit_per_file} chars>"
        sections.append(f"[FILE] {resolved}\n{text}\n[END FILE] {resolved}")
    return "\n\n".join(sections)


def _build_claude_command(
    prompt: str,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
) -> list[str]:
    """Claude CLI 명령을 구성합니다."""
    command = [_resolve_cli_command("claude"), "-p", prompt]
    effective_allowed_tools = _resolve_claude_allowed_tools(allowed_tools_pattern)
    if effective_allowed_tools:
        command.extend(["--allowedTools", effective_allowed_tools])
    command.extend(extra_args)
    return command


def _build_codex_command(
    prompt: str,
    extra_args: list[str],
    codex_mcp_approved_tools: list[str] | None = None,
    codex_mcp_approved_write_tools: list[str] | None = None,
    approve_codex_mcp_writes: bool = False,
) -> list[str]:
    """Codex CLI 명령을 구성합니다."""
    command = [
        _resolve_cli_command("codex"),
        *_build_codex_mcp_approval_config_args(
            codex_mcp_approved_tools,
            codex_mcp_approved_write_tools,
            approve_writes=approve_codex_mcp_writes,
        ),
        "--sandbox",
        _get_codex_sandbox_mode(),
        "--ask-for-approval",
        _get_codex_approval_policy(),
        "exec",
        "--skip-git-repo-check",
    ]
    for add_dir in _get_codex_add_dirs():
        command.extend(["--add-dir", add_dir])
    command.extend(extra_args)
    command.append(prompt)
    return command


def _build_command(
    agent: str,
    prompt: str,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
    codex_mcp_approved_tools: list[str] | None = None,
    codex_mcp_approved_write_tools: list[str] | None = None,
    approve_codex_mcp_writes: bool = False,
    *,
    _internal: bool = False,
) -> list[str]:
    """에이전트 종류에 맞는 전체 CLI 명령을 구성합니다."""
    if not _internal:
        _validate_extra_args(extra_args)

    if agent == "claude":
        return _build_claude_command(prompt, allowed_tools_pattern, extra_args)

    if agent == "codex":
        return _build_codex_command(
            prompt, extra_args,
            codex_mcp_approved_tools,
            codex_mcp_approved_write_tools,
            approve_codex_mcp_writes,
        )

    raise ValueError(f"unsupported agent: {agent}")


def run_agent_cli(
    *,
    agent: str,
    prompt: str,
    cwd: str | None,
    timeout_ms: int,
    allowed_tools_pattern: str | None,
    extra_args: list[str],
    codex_mcp_approved_tools: list[str] | None = None,
    codex_mcp_approved_write_tools: list[str] | None = None,
    approve_codex_mcp_writes: bool = False,
    run_id: str | None = None,
    session_id: str | None = None,
    _internal: bool = False,
) -> dict[str, Any]:
    """CLI 프로세스를 실행하고 출력·상태·타임아웃을 관리합니다."""
    command = _build_command(
        agent,
        prompt,
        allowed_tools_pattern,
        extra_args,
        codex_mcp_approved_tools,
        codex_mcp_approved_write_tools,
        approve_codex_mcp_writes,
        _internal=_internal,
    )
    working_directory = str(Path(cwd).resolve()) if cwd else None
    _log("cli.request", agent=agent, cwd=working_directory, timeout_ms=timeout_ms,
         prompt=_truncate(prompt, 1000), allowed_tools_pattern=allowed_tools_pattern)

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
    last_activity = time.monotonic()
    start_monotonic = last_activity
    _activity_lock = threading.Lock()

    # 활성 레지스트리에 등록
    idle_timeout_sec = int(os.getenv("ORCH_IDLE_TIMEOUT_SEC", str(DEFAULT_IDLE_TIMEOUT_SEC)))
    alive_log_interval = int(os.getenv("ORCH_ALIVE_LOG_INTERVAL_SEC", str(DEFAULT_ALIVE_LOG_INTERVAL_SEC)))
    if run_id is not None:
        with _active_runs_lock:
            _active_runs[run_id] = {
                "runId": run_id,
                "agent": agent,
                "sessionId": session_id,
                "pid": process.pid,
                "cwd": working_directory,
                "startedAt": _now_iso(),
                "startMonotonic": start_monotonic,
                "timeoutMs": timeout_ms,
                "idleTimeoutSec": idle_timeout_sec,
                "aliveLogIntervalSec": alive_log_interval,
                "stdoutLines": 0,
                "stderrLines": 0,
                "lastActivity": last_activity,
            }

    def _touch_activity() -> None:
        """마지막 활동 시각을 갱신합니다."""
        nonlocal last_activity
        with _activity_lock:
            last_activity = time.monotonic()

    def _seconds_since_activity() -> float:
        """마지막 활동 이후 경과 시간을 반환합니다."""
        with _activity_lock:
            return time.monotonic() - last_activity

    def consume(stream: Any, buffer: list[str], label: str) -> None:
        """출력 스트림을 읽어 버퍼와 활동 상태를 갱신합니다."""
        try:
            for line in iter(stream.readline, ""):
                buffer.append(line)
                _touch_activity()
                # 레지스트리 카운터 갱신
                if run_id is not None:
                    with _active_runs_lock:
                        entry = _active_runs.get(run_id)
                        if entry is not None:
                            key = "stdoutLines" if label == "stdout" else "stderrLines"
                            entry[key] = len(buffer)
                            entry["lastActivity"] = time.monotonic()
                _log(f"cli.stream.{label}", level="DEBUG", line=line.rstrip("\n"))
        finally:
            stream.close()

    stdout_thread = threading.Thread(target=consume, args=(process.stdout, stdout_chunks, "stdout"), daemon=True)
    stderr_thread = threading.Thread(target=consume, args=(process.stderr, stderr_chunks, "stderr"), daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    # 활동도 기반 타임아웃: 하드 타임아웃 안에서 idle 여부를 판정
    hard_deadline = time.monotonic() + (timeout_ms / 1000)
    last_alive_log = time.monotonic()

    timed_out = False
    timeout_reason = ""
    try:
        while True:
            try:
                exit_code = process.wait(timeout=2.0)
                break  # 프로세스 정상 종료
            except subprocess.TimeoutExpired:
                pass

            now = time.monotonic()
            idle_sec = _seconds_since_activity()
            elapsed_sec = now - (hard_deadline - timeout_ms / 1000)

            # 주기적으로 상태 로그 출력
            if now - last_alive_log >= alive_log_interval:
                _log("cli.alive", agent=agent,
                     elapsed_sec=round(elapsed_sec, 1),
                     idle_sec=round(idle_sec, 1),
                     stdout_lines=len(stdout_chunks),
                     stderr_lines=len(stderr_chunks))
                last_alive_log = now

            # idle 타임아웃: 일정 시간 출력이 없을 때
            if idle_sec >= idle_timeout_sec:
                timed_out = True
                timeout_reason = f"idle for {idle_sec:.0f}s (no output)"
                break

            # 하드 타임아웃: 최대 실행 시간 초과
            if now >= hard_deadline:
                timed_out = True
                timeout_reason = f"hard timeout after {timeout_ms}ms"
                break

    except Exception:
        timed_out = True
        timeout_reason = "unexpected error during wait"

    try:
        if timed_out:
            process.terminate()
            try:
                exit_code = process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                exit_code = process.wait(timeout=2)
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)
            _log("cli.timeout", level="ERROR", agent=agent, timeout_ms=timeout_ms,
                 reason=timeout_reason,
                 idle_sec=round(_seconds_since_activity(), 1))
            raise TimeoutError(f"agent timed out: {timeout_reason}")
        else:
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

        result = {
            "stdout": "".join(stdout_chunks).strip(),
            "stderr": "".join(stderr_chunks).strip(),
            "exitCode": int(exit_code),
        }
        if result["exitCode"] == 0:
            _log("cli.response", agent=agent, exit_code=0,
                 response=_truncate(result["stdout"]))
        else:
            _log("cli.response", level="ERROR", agent=agent, exit_code=result["exitCode"],
                 response=_truncate(result["stdout"]),
                 error=_truncate(result["stderr"]))
        return result
    finally:
        # 활성 레지스트리에서 제거
        if run_id is not None:
            with _active_runs_lock:
                _active_runs.pop(run_id, None)




# ---------------------------------------------------------------------------
# Cron schedule helpers
# ---------------------------------------------------------------------------

# 수동 실행 전용을 나타내는 센티널 값입니다. 6필드 형식(초를 포함한 cron 관례 표기)입니다.
# _parse_cron(5필드)에 도달하기 전에 _is_manual_only_cron()에서 가드됩니다.
_MANUAL_ONLY_CRON = "- - - - - -"

_CRON_FIELD_RANGES: tuple[tuple[int, int], ...] = (
    (0, 59),   # 分
    (0, 23),   # 時
    (1, 31),   # 日
    (1, 12),   # 月
    (0, 6),    # 曜日（月曜=0）
)


def _parse_cron_field(field: str, minimum: int, maximum: int) -> set[int]:
    """cron 단일 필드를 허용 값 집합으로 확장합니다."""
    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            raise ValueError("cron field contains an empty segment")
        if "/" in part:
            base, step_raw = part.split("/", 1)
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
            start, end = int(start_raw), int(end_raw)
        else:
            start = end = int(base)
        if start < minimum or end > maximum or start > end:
            raise ValueError(f"cron value out of range: {part}")
        values.update(range(start, end + 1, step))
    return values


def _parse_cron(expr: str) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
    """cron 식을 분·시·일·월·요일 값 집합으로 해석합니다."""
    fields = expr.strip().split()
    if len(fields) != 5:
        raise ValueError("cron expression must have 5 fields")
    parsed = tuple(
        _parse_cron_field(field, minimum, maximum)
        for field, (minimum, maximum) in zip(fields, _CRON_FIELD_RANGES, strict=True)
    )
    return parsed  # type: ignore[return-value]


def _is_manual_only_cron(expr: str) -> bool:
    """수동 실행 전용 cron 표현식인지 판단합니다."""
    stripped = expr.strip()
    if not stripped:
        return False
    return len(stripped.replace(" ", "")) >= 5 and all(char in {"-", " "} for char in stripped)


def _is_manual_only_prompt(prompt: str) -> bool:
    """프롬프트에 수동 실행 전용 마커가 있는지 판단합니다."""
    return _MANUAL_ONLY_CRON in prompt


def _cron_matches(expr: str, candidate_utc: datetime) -> bool:
    """지정 시각이 cron 조건과 일치하는지 확인합니다."""
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
    """다음 cron 실행 시각을 계산합니다."""
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
    """SQLite 데이터베이스에 연결하고 초기화합니다."""
    connection = sqlite3.connect(db_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    initialize_database(connection)
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    """세션·메시지·스케줄용 데이터베이스 스키마를 초기화합니다."""
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

        CREATE TABLE IF NOT EXISTS workflow_runs (
          id TEXT PRIMARY KEY,
          title TEXT,
          objective TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          metadata_json TEXT
        );

        CREATE TABLE IF NOT EXISTS workflow_decisions (
          id TEXT PRIMARY KEY,
          workflow_id TEXT NOT NULL,
          sequence INTEGER NOT NULL,
          stage TEXT NOT NULL,
          role TEXT NOT NULL,
          agent TEXT,
          source_run_id TEXT,
          source_session_id TEXT,
          expected_decision TEXT,
          decision TEXT,
          summary TEXT,
          findings_json TEXT,
          next_action TEXT,
          evidence_summary TEXT,
          prompt_summary TEXT,
          prompt_hash TEXT,
          status TEXT,
          created_at TEXT NOT NULL,
          metadata_json TEXT,
          FOREIGN KEY(workflow_id) REFERENCES workflow_runs(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_messages_session_id_sort_order ON messages(session_id, sort_order);
        CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(enabled, running, next_run_at);
        CREATE INDEX IF NOT EXISTS idx_scheduled_runs_job_started ON scheduled_runs(job_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_workflow_runs_updated ON workflow_runs(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_workflow_decisions_workflow_seq
          ON workflow_decisions(workflow_id, sequence);
        CREATE INDEX IF NOT EXISTS idx_workflow_decisions_source_run
          ON workflow_decisions(source_run_id);
    """)
    connection.commit()


# ---------------------------------------------------------------------------
# Prompt compilation for agent execution
# ---------------------------------------------------------------------------

def _compile_agent_prompt(
    *,
    agent: str,
    request_messages: list[dict[str, str]],
    injected_file_paths: list[str],
    resolved_cwd: str,
    batch_mode: bool,
) -> str:
    """에이전트 종류에 맞는 프롬프트를 구성합니다.

    Claude의 경우 filePaths 주입을 수행하고, 보호 경로 검증은 _read_files_for_prompt 내부에서 처리합니다.
    batch_mode일 때는 최종적으로 build_batch_prompt를 한 번만 적용합니다.
    """
    if agent == "claude":
        compiled = compile_claude_parts(request_messages)
        if injected_file_paths:
            injected_files = _read_files_for_prompt(
                injected_file_paths,
                base_dir=Path(resolved_cwd),
            )
            compiled = f"{compiled}\n\n[INJECTED FILE CONTENTS]\n{injected_files}".strip()
    else:
        compiled = compile_codex_prompt(request_messages)

    if batch_mode:
        compiled = build_batch_prompt(compiled)
    return compiled


def _build_scheduler_skip_permissions_args(job: dict[str, Any]) -> list[str]:
    """스케줄러 실행 시 skipPermissions extra args를 생성합니다.

    Claude에만 --dangerously-skip-permissions를 적용합니다.
    """
    if job.get("skipPermissions") and job["agent"] == "claude":
        return ["--dangerously-skip-permissions"]
    return []


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------

class SessionStore:
    """세션, 메시지, 스케줄, 실행 결과를 저장하는 저장소입니다."""
    def __init__(self, *, connection: sqlite3.Connection, db_path: str, default_timeout_ms: int) -> None:
        """SessionStore 인스턴스를 초기화합니다."""
        self.connection = connection
        self.db_path = db_path
        self.default_timeout_ms = default_timeout_ms
        self.lock = threading.RLock()
        self.active_run_lock = threading.Condition(threading.RLock())
        self.active_run_count = 0

    # -- serialization -------------------------------------------------------

    @staticmethod
    def _serialize_session(row: sqlite3.Row | None) -> dict[str, Any] | None:
        """session 행을 API 응답용 dict로 변환합니다."""
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
        """message 행을 API 응답용 dict로 변환합니다."""
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
        """세션 존재 여부를 확인합니다."""
        return self.connection.execute(
            "SELECT id FROM sessions WHERE id = ?", (session_id,)
        ).fetchone() is not None

    def _next_order(self, session_id: str) -> int:
        """다음 메시지 순서를 계산합니다."""
        row = self.connection.execute(
            "SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM messages WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return int(row["max_order"]) + 1

    def list_messages(self, session_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """메시지 목록을 반환합니다."""
        limit = max(1, min(limit, 500))
        with self.lock:
            if session_id:
                rows = self.connection.execute(
                    "SELECT id, session_id, role, content, agent, created_at, sort_order, is_session "
                    "FROM messages WHERE session_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                    (session_id, limit),
                ).fetchall()
            else:
                rows = self.connection.execute(
                    "SELECT id, session_id, role, content, agent, created_at, sort_order, is_session "
                    "FROM messages ORDER BY created_at DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [self._serialize_message(row) for row in rows]

    def _resolve_session(self, agent: str, session_id: str | None) -> str:
        """실행에 사용할 session ID를 결정합니다."""
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
        """현재 요청용 메시지 목록을 구성합니다."""
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
        """CLI에 전달할 대화 메시지를 구성합니다."""
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
        """새 세션을 생성합니다."""
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
        """세션과 메시지 이력을 조회합니다."""
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
        """최근 세션 목록을 조회합니다."""
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
        """기존 세션에 메시지를 추가합니다."""
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
        """세션을 삭제합니다."""
        with self.lock:
            if not self._session_exists(session_id):
                return {"deleted": False, "sessionId": session_id}
            self.connection.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            self.connection.commit()
        return {"deleted": True, "sessionId": session_id}

    def get_health(self) -> dict[str, Any]:
        """저장소와 데이터베이스 상태를 반환합니다."""
        return {
            "ok": True,
            "dbPath": self.db_path,
            "defaultTimeoutMs": self.default_timeout_ms,
        }

    # -- workflow decision log API ------------------------------------------

    @staticmethod
    def _serialize_workflow(row: sqlite3.Row | None) -> dict[str, Any] | None:
        """workflow_runs 행을 API 응답용 dict로 변환한다."""
        if row is None:
            return None
        return {
            "id": row["id"],
            "title": row["title"],
            "objective": row["objective"],
            "status": row["status"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "metadata": _json_loads_from_db(row["metadata_json"]),
        }

    @staticmethod
    def _serialize_workflow_decision(row: sqlite3.Row) -> dict[str, Any]:
        """workflow_runs 행을 API 응답용 dict로 변환한다."""
        return {
            "id": row["id"],
            "workflowId": row["workflow_id"],
            "sequence": row["sequence"],
            "stage": row["stage"],
            "role": row["role"],
            "agent": row["agent"],
            "sourceRunId": row["source_run_id"],
            "sourceSessionId": row["source_session_id"],
            "expectedDecision": row["expected_decision"],
            "decision": row["decision"],
            "summary": row["summary"],
            "findings": _json_loads_from_db(row["findings_json"]) or [],
            "nextAction": row["next_action"],
            "evidenceSummary": row["evidence_summary"],
            "promptSummary": row["prompt_summary"],
            "promptHash": row["prompt_hash"],
            "status": row["status"],
            "createdAt": row["created_at"],
            "metadata": _json_loads_from_db(row["metadata_json"]),
        }

    def create_workflow(
        self,
        *,
        title: str | None = None,
        objective: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """구조화된 작업 판단 로그를 담을 workflow를 생성한다."""
        workflow_id = str(uuid4())
        ts = _now_iso()
        with self.lock:
            self.connection.execute(
                "INSERT INTO workflow_runs (id, title, objective, status, created_at, updated_at, metadata_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    workflow_id,
                    _limit_text(title, 300),
                    _limit_text(objective, 2000),
                    "active",
                    ts,
                    ts,
                    _json_dumps_for_db(metadata),
                ),
            )
            self.connection.commit()
        workflow = self.get_workflow(workflow_id, include_decisions=False)
        return {"workflow": workflow}

    def append_workflow_decision(
        self,
        *,
        workflow_id: str,
        stage: str,
        role: str,
        agent: str | None = None,
        source_run_id: str | None = None,
        source_session_id: str | None = None,
        expected_decision: str | None = None,
        decision: str | None = None,
        summary: str | None = None,
        findings: list[dict[str, Any]] | None = None,
        next_action: str | None = None,
        evidence_summary: str | None = None,
        prompt_summary: str | None = None,
        prompt_hash: str | None = None,
        status: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """workflow에 stage/role 단위 판단 기록을 추가한다."""
        if not workflow_id:
            raise ValueError("workflowId is required")
        normalized_stage = _normalize_workflow_stage(stage)
        normalized_role = _normalize_workflow_role(role)
        if agent is not None and agent not in {"claude", "codex", "main"}:
            raise ValueError("agent must be claude, codex, main, or null")
        if findings is not None and not isinstance(findings, list):
            raise ValueError("findings must be a list")
        ts = _now_iso()
        decision_id = str(uuid4())
        with self.lock:
            row = self.connection.execute(
                "SELECT id FROM workflow_runs WHERE id = ?",
                (workflow_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"workflow not found: {workflow_id}")
            seq_row = self.connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS max_sequence FROM workflow_decisions WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchone()
            sequence = int(seq_row["max_sequence"]) + 1
            self.connection.execute(
                "INSERT INTO workflow_decisions "
                "(id, workflow_id, sequence, stage, role, agent, source_run_id, source_session_id, "
                "expected_decision, decision, summary, findings_json, next_action, evidence_summary, "
                "prompt_summary, prompt_hash, status, created_at, metadata_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    decision_id,
                    workflow_id,
                    sequence,
                    normalized_stage,
                    normalized_role,
                    agent,
                    source_run_id,
                    source_session_id,
                    _limit_text(expected_decision, 200),
                    _limit_text(decision, 200),
                    _limit_text(summary, 2000),
                    _json_dumps_for_db(findings or []),
                    _limit_text(next_action, 1000),
                    _limit_text(evidence_summary, 2000),
                    _limit_text(prompt_summary, 500),
                    _normalize_prompt_hash(prompt_hash),
                    _limit_text(status, 200),
                    ts,
                    _json_dumps_for_db(metadata),
                ),
            )
            self.connection.execute(
                "UPDATE workflow_runs SET updated_at = ? WHERE id = ?",
                (ts, workflow_id),
            )
            self.connection.commit()
            row = self.connection.execute(
                "SELECT * FROM workflow_decisions WHERE id = ?",
                (decision_id,),
            ).fetchone()
        return {"decision": self._serialize_workflow_decision(row)}

    def get_workflow(
        self,
        workflow_id: str,
        *,
        include_decisions: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any] | None:
        """workflow 본문과 선택적 decision 이력을 조회한다."""
        if not workflow_id:
            raise ValueError("workflowId is required")
        safe_limit = max(1, min(int(limit or 50), 500))
        safe_offset = max(0, int(offset or 0))
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM workflow_runs WHERE id = ?",
                (workflow_id,),
            ).fetchone()
            workflow = self._serialize_workflow(row)
            if workflow is None:
                return None
            count_row = self.connection.execute(
                "SELECT COUNT(*) AS count FROM workflow_decisions WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchone()
            workflow["decisionCount"] = int(count_row["count"])
            if include_decisions:
                decision_rows = self.connection.execute(
                    "SELECT * FROM workflow_decisions WHERE workflow_id = ? "
                    "ORDER BY sequence ASC LIMIT ? OFFSET ?",
                    (workflow_id, safe_limit, safe_offset),
                ).fetchall()
                workflow["decisions"] = [self._serialize_workflow_decision(decision) for decision in decision_rows]
                workflow["limit"] = safe_limit
                workflow["offset"] = safe_offset
        return workflow

    def list_workflows(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        """최근 업데이트 순으로 workflow 목록을 조회한다."""
        safe_limit = max(1, min(int(limit or 20), 100))
        with self.lock:
            if status:
                rows = self.connection.execute(
                    "SELECT * FROM workflow_runs WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                    (status, safe_limit),
                ).fetchall()
            else:
                rows = self.connection.execute(
                    "SELECT * FROM workflow_runs ORDER BY updated_at DESC LIMIT ?",
                    (safe_limit,),
                ).fetchall()
        return [workflow for row in rows if (workflow := self._serialize_workflow(row)) is not None]

    def _maybe_record_workflow_decision(
        self,
        *,
        workflow: dict[str, Any] | None,
        agent: str,
        run_id: str,
        session_id: str,
        status: str,
        prompt: str,
        result: dict[str, Any],
    ) -> dict[str, Any] | None:
        """agent_run 결과를 workflow decision으로 자동 기록할 수 있으면 기록한다."""
        if not workflow:
            return None
        workflow_id = str(workflow.get("id") or workflow.get("workflowId") or "").strip()
        if not workflow_id:
            return None
        prompt_summary = workflow.get("promptSummary") or workflow.get("prompt_summary")
        decision = workflow.get("decision") or ("completed" if status == "completed" else status)
        summary = workflow.get("summary") or _limit_text(result.get("stdout"), 500)
        return self.append_workflow_decision(
            workflow_id=workflow_id,
            stage=str(workflow.get("stage") or "execute"),
            role=str(workflow.get("role") or "agent"),
            agent=agent,
            source_run_id=run_id,
            source_session_id=session_id,
            expected_decision=workflow.get("expectedDecision") or workflow.get("expected_decision"),
            decision=decision,
            summary=summary,
            findings=workflow.get("findings") if isinstance(workflow.get("findings"), list) else [],
            next_action=workflow.get("nextAction") or workflow.get("next_action"),
            evidence_summary=workflow.get("evidenceSummary") or workflow.get("evidence_summary"),
            prompt_summary=prompt_summary,
            prompt_hash=_workflow_prompt_hash(prompt),
            status=status,
            metadata=workflow.get("metadata") if isinstance(workflow.get("metadata"), dict) else None,
        )

    # -- schedule API --------------------------------------------------------

    @staticmethod
    def _serialize_schedule(row: sqlite3.Row | None) -> dict[str, Any] | None:
        """schedule 행을 API 응답용 dict로 변환합니다."""
        if row is None:
            return None
        prompt = row["prompt"]
        return {
            "id": row["id"],
            "name": row["name"],
            "agent": row["agent"],
            "cronExpr": row["cron_expr"],
            "prompt": prompt,
            "manualOnly": _is_manual_only_prompt(str(prompt)) or _is_manual_only_cron(str(row["cron_expr"])),
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
        """schedule run 행을 API 응답용 dict로 변환합니다."""
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
        """새 스케줄 작업을 생성합니다."""
        if agent not in {"claude", "codex"}:
            raise ValueError("agent must be claude or codex")
        if not name.strip():
            raise ValueError("name is required")
        if not prompt.strip():
            raise ValueError("prompt is required")
        is_manual = _is_manual_only_prompt(prompt) or _is_manual_only_cron(cron_expr)
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
        """스케줄 작업 상세를 조회합니다."""
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM scheduled_jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return self._serialize_schedule(row)

    def list_schedules(self, include_disabled: bool = True) -> list[dict[str, Any]]:
        """스케줄 작업 목록을 조회합니다."""
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
        """기존 스케줄 작업을 갱신합니다."""
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
        is_manual = _is_manual_only_prompt(str(effective_prompt)) or _is_manual_only_cron(str(effective_cron))
        if cron_expr is not None or prompt is not None:
            values["next_run_at"] = None if is_manual else _next_cron_run(str(effective_cron))
        if enabled is not None:
            values["enabled"] = 1 if enabled else 0
            if enabled and "next_run_at" not in values:
                values["next_run_at"] = None if is_manual else _next_cron_run(str(effective_cron))
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
        """스케줄 작업을 삭제합니다."""
        with self.lock:
            row = self.connection.execute("SELECT id FROM scheduled_jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                return {"deleted": False, "jobId": job_id}
            self.connection.execute("DELETE FROM scheduled_jobs WHERE id = ?", (job_id,))
            self.connection.commit()
        return {"deleted": True, "jobId": job_id}

    def list_due_schedule_ids(self, limit: int = 5) -> list[str]:
        """실행 시각이 된 스케줄 ID 목록을 조회합니다."""
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
        """스케줄 실행 이력을 조회합니다."""
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
        """실행 중 스케줄 수를 증가시킵니다."""
        with self.active_run_lock:
            self.active_run_count += 1

    def _end_schedule_execution(self) -> None:
        """실행 중 스케줄 수를 감소시킵니다."""
        with self.active_run_lock:
            self.active_run_count = max(0, self.active_run_count - 1)
            self.active_run_lock.notify_all()

    def wait_for_schedule_executions(self, timeout_seconds: float) -> bool:
        """실행 중 스케줄이 끝날 때까지 대기합니다."""
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
        """스케줄 실행 결과를 DB에 반영합니다."""
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
            extra_args = _build_scheduler_skip_permissions_args(job)
            result = self.run_agent(
                agent=job["agent"],
                prompt=job["prompt"],
                extra_args=extra_args if extra_args else None,
                batch_mode=True,
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
                next_run_at = None if job.get("manualOnly") else _next_cron_run(job["cronExpr"])
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
        """스케줄 작업을 실행합니다."""
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
        allowed_tools_pattern: str | None = None,
        codex_mcp_approved_tools: list[str] | None = None,
        codex_mcp_approved_write_tools: list[str] | None = None,
        approve_codex_mcp_writes: bool = False,
        cwd: str | None = None,
        timeout_ms: int | None = None,
        extra_args: list[str] | None = None,
        workflow: dict[str, Any] | None = None,
        run_id: str | None = None,
        batch_mode: bool = False,
        _internal: bool = False,
    ) -> dict[str, Any]:
        """세션 기반 agent 실행을 수행합니다."""
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

        compiled_prompt = _compile_agent_prompt(
            agent=agent,
            request_messages=request_messages,
            injected_file_paths=injected_file_paths,
            resolved_cwd=resolved_cwd,
            batch_mode=batch_mode,
        )

        _log(
            "agent.compiled_prompt",
            agent=agent,
            session_id=active_session_id,
            use_session=use_session,
            cwd=resolved_cwd,
            compiled_prompt=_truncate(compiled_prompt),
        )

        run_id = run_id or str(uuid4())
        effective_timeout_ms = int(timeout_ms or self.default_timeout_ms)
        result = run_agent_cli(
            agent=agent,
            prompt=compiled_prompt,
            cwd=resolved_cwd,
            timeout_ms=effective_timeout_ms,
            allowed_tools_pattern=allowed_tools_pattern,
            extra_args=extra_args or [],
            codex_mcp_approved_tools=codex_mcp_approved_tools,
            codex_mcp_approved_write_tools=codex_mcp_approved_write_tools,
            approve_codex_mcp_writes=approve_codex_mcp_writes,
            run_id=run_id,
            session_id=active_session_id,
            _internal=_internal,
        )
        status = "completed" if result["exitCode"] == 0 else "failed"
        mcp_tool_cancelled = _is_mcp_tool_cancelled(result["stdout"], result["stderr"])
        if mcp_tool_cancelled:
            status = "mcp_tool_cancelled"

        assistant_content = (
            result["stdout"] or "(empty response)"
            if status == "completed"
            else f"[{status.upper()} exit={result['exitCode']}] "
                 f"{result['stderr'] or result['stdout'] or '(no output)'}"
        )
        self.append_messages(
            active_session_id,
            [{"role": "assistant", "content": assistant_content}],
            agent=agent,
            is_session=use_session,
        )
        _log("agent.session_saved", session_id=active_session_id, agent=agent, status=status)

        payload: dict[str, Any] = {
            "runId": run_id,
            "sessionId": active_session_id,
            "agent": agent,
            "exitCode": result["exitCode"],
            "status": status,
            "stdout": result["stdout"],
            "stderr": result["stderr"],
        }
        if mcp_tool_cancelled:
            payload["failureReason"] = "mcp_tool_call_cancelled"
            payload["guidance"] = _mcp_tool_cancelled_guidance(agent, allowed_tools_pattern)
        if injected_file_paths:
            payload["filePaths"] = injected_file_paths
        if workflow:
            try:
                workflow_result = self._maybe_record_workflow_decision(
                    workflow=workflow,
                    agent=agent,
                    run_id=run_id,
                    session_id=active_session_id,
                    status=status,
                    prompt=compiled_prompt,
                    result=result,
                )
                if workflow_result is not None:
                    payload["workflowDecision"] = workflow_result["decision"]
            except Exception as exc:
                payload["workflowLogError"] = str(exc)
                _log("workflow.auto_append_failed", run_id=run_id, session_id=active_session_id, error=str(exc))
        if _env_flag("ORCH_DEBUG", True):
            payload["compiledPrompt"] = compiled_prompt
        _remember_completed_run(run_id, payload)
        _log("agent.result", result=_truncate(_safe_json(payload)))
        return payload




# ---------------------------------------------------------------------------
# Local scheduler and Web UI
# ---------------------------------------------------------------------------

class ScheduleRunner:
    """로컬 cron 작업을 백그라운드에서 실행하는 실행기."""

    def __init__(self, *, store: SessionStore, interval_seconds: int = 30) -> None:
        """ScheduleRunner 인스턴스를 초기화합니다."""
        self.store = store
        self.interval_seconds = max(5, int(interval_seconds or 30))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="orch-scheduler", daemon=True)

    def start(self) -> None:
        """스케줄 감시 스레드를 시작합니다."""
        self._thread.start()
        _log("schedule.runner_started", interval_seconds=self.interval_seconds)

    def stop(self) -> None:
        """스케줄 감시 스레드를 중지합니다."""
        self._stop.set()
        self._thread.join(timeout=5)
        _log("schedule.runner_stopped")

    def _loop(self) -> None:
        """주기적으로 실행 시각이 된 스케줄을 감시합니다."""
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


def _nav_links(active: str = "") -> str:
    """탐색 링크 HTML을 생성합니다."""
    items = [
        ("Dashboard", "/"),
        ("Runs", "/runs"),
        ("Messages", "/messages"),
    ]
    parts = []
    for label, href in items:
        if label.lower() == active.lower():
            parts.append(f"<strong>{label}</strong>")
        else:
            parts.append(f'<a href="{href}">{label}</a>')
    return " | ".join(parts)


def _html_page(title: str, body: str) -> bytes:
    """공통 HTML 페이지를 렌더링합니다."""
    return f"""<!doctype html>
<html lang="ko">
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

    @media (max-width: 900px) {{
      .stats {{ grid-template-columns: repeat(2, 1fr); }}
      .main-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
{body}

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

<div id="toast" style="position:fixed;top:24px;right:24px;z-index:2000;display:flex;flex-direction:column;gap:8px;"></div>
<div id="loading-overlay" style="display:none;position:fixed;inset:0;background:rgba(15,25,35,0.7);z-index:3000;align-items:center;justify-content:center;flex-direction:column;gap:16px;">
  <div style="width:40px;height:40px;border:3px solid var(--border);border-top-color:var(--accent-blue);border-radius:50%;animation:spin 0.8s linear infinite;"></div>
  <div id="loading-text" style="color:var(--text-secondary);font-size:0.9rem;">실행 중...</div>
</div>
<style>@keyframes spin {{ from {{ transform: rotate(0deg); }} to {{ transform: rotate(360deg); }} }}</style>

<script>
function showLoading(text) {{
  document.getElementById('loading-text').textContent = text || '실행 중...';
  document.getElementById('loading-overlay').style.display = 'flex';
}}
function hideLoading() {{
  document.getElementById('loading-overlay').style.display = 'none';
}}

function showToast(message, type) {{
  const toast = document.getElementById('toast');
  const el = document.createElement('div');
  const bg = type === 'error' ? 'var(--accent-red)' : 'var(--accent-green)';
  el.style.cssText = 'padding:12px 20px;border-radius:8px;color:#fff;font-size:0.9rem;max-width:400px;word-break:break-word;opacity:0;transition:opacity 0.3s;background:' + bg;
  el.textContent = message;
  toast.appendChild(el);
  requestAnimationFrame(() => {{ el.style.opacity = '1'; }});
  setTimeout(() => {{
    el.style.opacity = '0';
    setTimeout(() => el.remove(), 300);
  }}, 4000);
}}

function submitForm(form) {{
  showLoading();
  const formData = new URLSearchParams(new FormData(form));
	  fetch(form.action, {{
	    method: 'POST',
	    headers: {{
	      'X-Requested-With': 'fetch',
	      'Accept': 'application/json',
	      'Content-Type': 'application/x-www-form-urlencoded'
	    }},
	    body: formData.toString(),
	  }})
    .then(r => {{
      if (r.ok) {{
        window.location.reload();
      }} else {{
        hideLoading();
        return r.json().then(data => showToast(data.error || 'Request failed', 'error'));
      }}
    }})
    .catch(err => {{
      hideLoading();
      showToast(err.message || 'Network error', 'error');
    }});
}}

document.querySelectorAll('form[method="post"]').forEach(form => {{
  form.addEventListener('submit', function(e) {{
    if (form.action.includes('/schedule/run')) return;
    e.preventDefault();
    if (form.action.includes('/schedule/delete') && !confirm('Delete this schedule?')) return;
    submitForm(form);
  }});
}});

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
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
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
          showToast(payload.error || '실행에 실패했습니다', 'error');
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

(function() {{
  var _pollTimer = null;
  var _pollFailureCount = 0;
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
    fetch('/api/jobs', {{ headers: {{ 'Accept': 'application/json' }} }})
      .then(function(r) {{ return r.json(); }})
      .then(function(data) {{
        _pollFailureCount = 0;
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
            span.textContent = '実行中 ' + runningCount + '件';
            subtitle.appendChild(document.createTextNode(' — '));
            subtitle.appendChild(span);
          }} else if (label) {{
            label.textContent = '実行中 ' + runningCount + '件';
          }}
        }} else {{
          hideLoading();
          if (label) {{
            var previous = label.previousSibling;
            if (previous) previous.remove();
            label.remove();
          }}
          clearInterval(_pollTimer);
          _pollTimer = null;
        }}
      }})
      .catch(function() {{
        _pollFailureCount += 1;
        if (_pollFailureCount >= 3) {{
          clearInterval(_pollTimer);
          _pollTimer = null;
          hideLoading();
          showToast('작업 상태를 가져오지 못했습니다. 페이지를 다시 불러와 주세요.', 'error');
        }}
      }});
  }}

  function _startPolling() {{
    if (_pollTimer) return;
    _pollTimer = setInterval(_pollJobs, 4000);
    _pollJobs();
  }}

  if (document.querySelector('[data-status-badge].running')) {{
    showLoading(_runLoadingText);
    _startPolling();
  }}

  window._startJobPolling = _startPolling;
}})();

function openEditModal(jobId) {{
  fetch('/api/schedule?jobId=' + encodeURIComponent(jobId))
    .then(r => r.json())
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
    }});
}}
function closeEditModal() {{
  document.getElementById('editModal').classList.remove('active');
}}
document.getElementById('editModal').addEventListener('click', function(e) {{
  if (e.target === this) closeEditModal();
}});
</script>
</body>
</html>""".encode("utf-8")


def _form_bool(values: dict[str, list[str]], key: str, default: bool = False) -> bool:
    """폼 값을 불리언으로 해석합니다."""
    if key not in values:
        return default
    return str(values.get(key, [""])[0]).lower() in {"1", "true", "on", "yes"}


def _form_int(values: dict[str, list[str]], key: str) -> int | None:
    """폼 값을 정수로 해석합니다."""
    raw = values.get(key, [""])[0].strip()
    return int(raw) if raw else None


class WebUiHandler(BaseHTTPRequestHandler):
    """간단한 Web UI 요청을 처리하는 핸들러입니다."""
    server_version = "OrchestrationWebUI/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        """기본 접근 로그를 구조화 로그로 기록합니다."""
        _log("web.access", client=self.client_address[0], message=fmt % args)

    @property
    def store(self) -> SessionStore:
        """초기화된 SessionStore 인스턴스를 반환합니다."""
        return _get_store()

    def _send(self, status: HTTPStatus, body: str | bytes, content_type: str = "text/html; charset=utf-8") -> None:
        """HTTP 응답 본문을 전송합니다."""
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str = "/") -> None:
        """지정 경로로 리다이렉트합니다."""
        self.send_response(HTTPStatus.SEE_OTHER.value)
        self.send_header("Location", location)
        self.end_headers()

    def _read_form(self) -> dict[str, list[str]]:
        """POST form 데이터를 읽습니다."""
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        return parse_qs(raw, keep_blank_values=True)

    def _prefers_json(self) -> bool:
        """JSON 응답 선호 여부를 판단합니다."""
        requested_with = self.headers.get("X-Requested-With", "")
        accept = self.headers.get("Accept", "")
        return requested_with.lower() == "fetch" or "application/json" in accept.lower()

    def do_GET(self) -> None:  # noqa: N802
        """HTTP GET 요청을 처리합니다."""
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send(HTTPStatus.OK, json.dumps(self.store.get_health(), ensure_ascii=False), "application/json; charset=utf-8")
            return
        if parsed.path == "/runs":
            query = parse_qs(parsed.query)
            self._render_runs(query.get("jobId", [None])[0])
            return
        if parsed.path == "/messages":
            query = parse_qs(parsed.query)
            session_id = query.get("sessionId", [None])[0]
            limit_raw = query.get("limit", ["100"])[0]
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
            self._render_messages(session_id, limit)
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
        """HTTP POST 요청을 처리합니다."""
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
                self._send(HTTPStatus.BAD_REQUEST, _json_response({"ok": False, "error": str(exc)}), "application/json; charset=utf-8")
            else:
                self._send(HTTPStatus.BAD_REQUEST, _html_page("Error", f"<h1>요청 실패</h1><p class='danger'>{html.escape(str(exc))}</p><p><a href='/'>돌아가기</a></p>"))

    def _render_index(self) -> None:
        """대시보드 메인 페이지를 렌더링합니다."""
        jobs = self.store.list_schedules(include_disabled=True)
        runs = self.store.list_schedule_runs(job_id=None, limit=5)

        # 통계 계산
        active_count = sum(1 for j in jobs if j["enabled"])
        total_runs = len(runs)
        failed_runs = sum(1 for r in runs if r["status"] == "failed")
        agents_used = len(set(j["agent"] for j in jobs)) if jobs else 0

        # 최근 실행 이력 목록
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

        # 스케줄 목록
        schedule_items = []
        for job in jobs:
            status = "running" if job["running"] else ("enabled" if job["enabled"] else "disabled")
            toggle_value = "0" if job["enabled"] else "1"
            toggle_btn_class = "btn-orange" if job["enabled"] else "btn-green"
            toggle_label = "Pause" if job["enabled"] else "Enable"
            agent_class = html.escape(job["agent"])
            prompt_preview = _truncate(job["prompt"], 80)
            cron_display = "수동 실행만" if _is_manual_only_cron(job["cronExpr"]) else html.escape(job["cronExpr"])
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

        nav = _nav_links("dashboard")
        body = f"""
<div class="header">
  <div>
    <h1>MCP Orchestration</h1>
    <span class="subtitle">스케줄 관리 대시보드 &nbsp;|&nbsp; {nav}</span>
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
        <input name="cronExpr" value="*/10 * * * *" placeholder="*/10 * * * * (수동 전용: - - - - - -)" required>
      </div>
      <div class="form-group">
        <label>Prompt</label>
        <textarea name="prompt" placeholder="실행할 프롬프트를 입력..." required></textarea>
      </div>
      <div class="checkbox-group">
        <input type="checkbox" name="skipPermissions" id="skip-perm-check">
        <label for="skip-perm-check">Skip Permissions (파일 쓰기 허용)</label>
      </div>
      <div class="checkbox-group">
        <input type="checkbox" name="enabled" id="enabled-check" checked>
        <label for="enabled-check">활성화해서 생성</label>
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
      {f'<ul class="activity-list">{"".join(activity_items)}</ul>' if activity_items else '<div class="empty">실행 이력 없음</div>'}
      <div style="margin-top:12px;"><a href="/runs">모든 실행 이력 보기 →</a></div>
    </div>
  </div>
</div>
"""
        self._send(HTTPStatus.OK, _html_page("MCP Orchestration", body))

    def _render_runs(self, job_id: str | None) -> None:
        """실행 이력 페이지를 렌더링합니다."""
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
        nav = _nav_links("runs")
        body = f"""
<div class="header">
  <div>
    <h1>실행 이력{filter_label}</h1>
    <span class="subtitle">{nav}</span>
  </div>
</div>
<div class="card">
  <table class="runs-table">
    <thead><tr><th>Run ID</th><th>Status</th><th>Exit</th><th>Started</th><th>Finished</th><th>Error/Stderr</th></tr></thead>
    <tbody>{''.join(rows) if rows else '<tr><td colspan="6" class="empty">실행 이력 없음</td></tr>'}</tbody>
  </table>
</div>
"""
        self._send(HTTPStatus.OK, _html_page("실행 이력", body))



    def _render_messages(self, session_id: str | None, limit: int) -> None:
        """메시지 목록 페이지를 렌더링합니다."""
        messages = self.store.list_messages(session_id=session_id, limit=limit)
        rows = []
        for msg in messages:
            content_raw = msg.get("content") or ""
            # role 미리보기
            content_preview = html.escape(_truncate(content_raw, 120))
            # content 전체 보기
            _CONTENT_DISPLAY_CAP = 1000
            if len(content_raw) > _CONTENT_DISPLAY_CAP:
                content_capped = html.escape(content_raw[:_CONTENT_DISPLAY_CAP]) + "\n…[truncated – showing first 1000 chars]"
            else:
                content_capped = html.escape(content_raw)
            show_details = len(content_raw) > 120
            content_cell = f"""<details><summary>{content_preview}</summary><pre style="white-space:pre-wrap;max-height:300px;overflow:auto;margin-top:6px;">{content_capped}</pre></details>""" if show_details else f"<span>{content_preview}</span>"
            rows.append(f"""
<tr>
  <td class="mono">{html.escape(str(msg['id'])[:12])}</td>
  <td class="mono">{html.escape(str(msg.get('createdAt') or '-'))}</td>
  <td class="mono">{html.escape(str(msg.get('sessionId') or '-')[:12])}</td>
  <td>{html.escape(str(msg.get('role') or '-'))}</td>
  <td>{html.escape(str(msg.get('agent') or '-'))}</td>
  <td>{msg.get('isSession', False)}</td>
  <td>{msg.get('order', 0)}</td>
  <td style="max-width:400px;overflow:hidden;">{content_cell}</td>
</tr>""")
        filter_label = f' — session: {html.escape(session_id[:12])}' if session_id else ""
        nav = _nav_links("messages")
        body = f"""
<div class="header">
  <div>
    <h1>Messages{filter_label}</h1>
    <span class="subtitle">{nav}</span>
  </div>
</div>
<div class="card">
  <table class="runs-table">
    <thead><tr><th>ID</th><th>Created</th><th>Session ID</th><th>Role</th><th>Agent</th><th>isSession</th><th>Order</th><th>Content</th></tr></thead>
    <tbody>{''.join(rows) if rows else '<tr><td colspan="8" class="empty">메시지 없음</td></tr>'}</tbody>
  </table>
</div>
"""
        self._send(HTTPStatus.OK, _html_page("Messages", body))


def _start_web_ui() -> None:
    """Web UI 서버를 시작합니다."""
    global _web_server, _web_thread
    if not _env_flag("ORCH_WEB_ENABLED", True):
        _log("web.disabled")
        return
    host = os.getenv("ORCH_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("ORCH_WEB_PORT", "18765"))
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
    """Web UI 서버를 중지합니다."""
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
    """프로젝트 루트 디렉터리를 반환합니다."""
    return Path(__file__).resolve().parents[1]


def _get_base_dir() -> Path:
    """CLI 실행 시 기본 작업 디렉터리를 반환한다."""
    return BASE_DIR


def _get_db_path() -> Path:
    """SQLite 데이터베이스 경로를 반환합니다."""
    if DB_PATH is not None:
        return DB_PATH.resolve()
    return Path(os.getenv("ORCH_DB_PATH", str(_get_root_dir() / "data" / "orchestrator.sqlite"))).resolve()


def _get_transport() -> Transport:
    """MCP 전송 방식을 반환합니다."""
    return "streamable-http"


async def _run_mcp_server_async() -> None:
    """비동기 MCP 서버를 실행합니다."""
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
    """초기화된 저장소 인스턴스를 반환합니다."""
    if _store is None:
        raise RuntimeError("session store is not initialized")
    return _store


def _mark_running_interrupted(connection: sqlite3.Connection, *, reason: str) -> None:
    """실행 중인 스케줄 작업을 중단 상태로 표시합니다."""
    now = _now_iso()
    connection.execute(
        "UPDATE scheduled_runs SET status = 'failed', finished_at = ?, error = ? WHERE status = 'running'",
        (now, reason),
    )
    connection.execute("UPDATE scheduled_jobs SET running = 0 WHERE running = 1")
    connection.commit()


def _reset_stuck_running_jobs(connection: sqlite3.Connection) -> None:
    """서버 재시작 시 이전 비정상 종료로 running=1이 남은 잡을 리셋한다."""
    _mark_running_interrupted(connection, reason="서버 재시작으로 중단되었습니다")
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
        port=int(os.getenv("ORCH_PORT", os.getenv("ORCH_SSE_PORT", "18282"))),
        web_enabled=_env_flag("ORCH_WEB_ENABLED", True),
        web_host=os.getenv("ORCH_WEB_HOST", "127.0.0.1"),
        web_port=int(os.getenv("ORCH_WEB_PORT", "18765")),
        log_dir=str(_get_log_dir()),
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
            _mark_running_interrupted(_connection, reason="서버 종료로 중단되었습니다")
        _connection.close()
    _connection = None
    _store = None


@contextlib.asynccontextmanager
async def server_lifespan(_: FastMCP) -> AsyncIterator[None]:
    """서버 lifespan 컨텍스트를 제공합니다."""
    yield


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

@mcp.tool(name="orchestrator_usage", description="이 MCP 서버의 사용 가이드를 반환합니다. AI가 도구를 올바르게 호출하기 위한 절차와 예시를 포함합니다.")
def orchestrator_usage() -> dict[str, Any]:
    """사용 가이드와 권장 절차를 반환합니다."""
    _log_tool_call("orchestrator_usage")
    guide = {
        "overview": (
            "이 서버는 Claude/Codex CLI를 세션 기반으로 실행하는 오케스트레이터입니다."
            " user/assistant 메시지 이력을 세션에 저장해 문맥을 유지한 대화를 이어갈 수 있습니다."
        ),
        "recommendedFlow": [
            "1. orchestrator_health로 서버 상태를 확인합니다(선택).",
            "2. session_create로 세션을 만들거나 agent_run이 자동으로 세션을 만들게 둡니다.",
            "3. agent_run으로 Claude/Codex에 질문을 보내고 응답을 받습니다.",
            "4. 같은 sessionId로 agent_run을 반복하면 대화가 이어집니다.",
            "5. session_get으로 과거 대화 이력을 확인할 수 있습니다.",
            "6. workflow_create / workflow_decision_append / workflow_get으로 판단 로그를 구조화해 저장할 수 있습니다.",
            "7. 120초를 넘길 수 있는 실행은 agent_run_start로 시작하고 agent_run_status로 polling 합니다.",
        ],
        "changeWorkflow": [
            "1. 변경 작업은 먼저 Codex로 구현 계획 초안을 만들고 대상 파일, 제약, 검증 방법을 명시합니다.",
            "2. 구현 전에 Claude로 plan-review를 받아 누락 범위, 리스크, 검증 관점, 승인 경계를 점검합니다.",
            "3. plan-review가 통과되거나 우려 사항이 해소되기 전까지는 대상 파일 쓰기를 시작하지 않습니다.",
            "4. 승인 후 계획 범위만 구현하고, 사용자 요청 밖 파일이나 금지 경로는 건드리지 않습니다.",
            "5. 구현 후 로컬 검증을 수행하고 문법, 차이점, 대상 파일 범위를 증거로 남깁니다.",
            "6. 최종 결과는 Codex와 Claude 양쪽에서 result-review 형태로 확인하고 status, findings, next action을 받습니다.",
            "7. 장시간 실행 중에는 agent_run_status로 프로세스/stdout/stderr 활동을 확인할 수 있습니다. 내부 모델 추론은 직접 관찰되지 않습니다.",
            "8. 리뷰가 fail 또는 blocking/major finding이면 수정 후 다시 리뷰하고 완료 보고합니다.",
        ],
        "tools": [
            {"name": "orchestrator_health", "purpose": "서버 상태 확인", "params": "없음"},
            {"name": "orchestrator_usage", "purpose": "이 사용 가이드 조회", "params": "없음"},
            {"name": "session_create", "purpose": "새 세션 생성 및 초기 메시지 저장", "params": {"title": "(선택) 세션 이름", "messages": "(선택) [{role: 'user'|'assistant', content: '...'}] 형식의 초기 메시지 배열"}},
            {"name": "session_get", "purpose": "세션 전체 메시지 이력 조회", "params": {"sessionId": "(필수) 세션 ID"}},
            {"name": "session_list", "purpose": "최근 세션 목록 조회", "params": {"limit": "(선택) 조회 개수. 기본 20, 최대 100"}},
            {"name": "session_append", "purpose": "기존 세션에 메시지를 수동 추가", "params": {"sessionId": "(필수) 세션 ID"}},
            {"name": "session_delete", "purpose": "세션과 전체 메시지 삭제", "params": {"sessionId": "(필수) 세션 ID"}},
            {"name": "workflow_create", "purpose": "계획/리뷰/실행/재리뷰 등의 판단을 묶는 workflow 생성", "params": {"title": "(선택) 제목", "objective": "(선택) 목표", "metadata": "(선택) raw prompt를 포함하지 않는 보조 JSON"}},
            {"name": "workflow_decision_append", "purpose": "workflow에 stage/role 단위 판단 로그 추가", "params": {"workflowId": "(필수) workflow_create가 반환한 ID", "stage": "(필수) plan / plan-review / execute / result-review / ng-fix / re-review 등", "role": "(필수) claude / codex / user / system", "decision": "(선택) approved / rejected / completed / needs-fix 등", "summary": "(선택) 판단 요약. raw prompt는 저장하지 않음"}},
            {"name": "workflow_get", "purpose": "workflow와 decision 이력 조회", "params": {"workflowId": "(필수) workflow ID", "includeDecisions": "(선택) decision 포함 여부. 기본 true", "limit": "(선택) decision 조회 수. 기본 50, 최대 500"}},
            {"name": "workflow_list", "purpose": "최근 업데이트된 workflow 목록 조회", "params": {"status": "(선택) active/completed/failed 등", "limit": "(선택) 기본 20, 최대 100"}},
            {"name": "agent_run", "purpose": "Claude 또는 Codex CLI 실행 후 결과를 세션에 저장", "params": {"agent": "(필수) 'claude' 또는 'codex'", "prompt": "(필수) 사용자 질문 텍스트", "promptBase64": "(선택) prompt를 Base64로 전달할 때 사용", "useSession": "(선택) true이면 세션 이력 사용. 기본 true", "sessionId": "(선택) 기존 세션 ID. 생략하면 새로 생성", "messages": "(선택) 추가 컨텍스트 메시지 [{role, content}]", "filePaths": "(선택) 서버가 UTF-8로 직접 읽어 Claude 프롬프트에 주입할 로컬 파일 경로 배열", "allowedToolsPattern": "(선택) Claude CLI에 전달할 허용 도구 패턴", "skipPermissions": "(선택) Claude 권한 체크 우회 여부. 기본 false", "codexMcpApprovedTools": "(선택) Codex에 사전 승인할 MCP tool의 server.tool 배열(read/범용)", "codexMcpApprovedWriteTools": "(선택) Codex에 사전 승인할 write MCP tool의 server.tool 배열", "approveCodexMcpWrites": "(선택) write MCP tool 사전 승인을 활성화하는 명시 플래그. 기본 false", "cwd": "(선택) CLI 실행 디렉터리", "timeoutMs": f"(선택) 하드 타임아웃 ms. 기본 {DEFAULT_TIMEOUT_MS}", "extraArgs": "(선택) CLI 추가 인수 배열", "workflow": "(선택) {id, stage, role, expectedDecision, promptSummary, decision, summary, findings, nextAction, evidenceSummary, metadata}"}, "returns": {"sessionId": "사용된 세션 ID", "status": "'completed' 또는 'failed'", "stdout": "CLI 표준 출력(응답 본문)", "stderr": "CLI 표준 오류 출력", "exitCode": "프로세스 종료 코드(0=성공)"}},
            {"name": "agent_run_start", "purpose": "장시간 실행용으로 Claude 또는 Codex CLI를 백그라운드 시작하고 runId/sessionId를 즉시 반환", "params": {"agent": "(필수) 'claude' 또는 'codex'", "prompt": "(필수) 사용자 질문 텍스트. promptBase64 사용 가능", "useSession": "(선택) 세션 이력 사용 여부. 기본 true", "sessionId": "(선택) 기존 세션 ID. 생략하면 시작 전에 새로 생성", "filePaths": "(선택) Claude prompt에 주입할 로컬 파일 경로 배열", "allowedToolsPattern": "(선택) Claude CLI 허용 도구 패턴", "skipPermissions": "(선택) Claude 권한 체크 우회 여부", "codexMcpApprovedTools": "(선택) Codex에 사전 승인할 MCP tool 배열", "approveCodexMcpWrites": "(선택) Codex write MCP 승인 활성화 여부", "cwd": "(선택) CLI 실행 디렉터리", "timeoutMs": f"(선택) 하드 타임아웃 ms. 기본 {DEFAULT_TIMEOUT_MS}"}, "returns": {"runId": "agent_run_status로 polling할 run ID", "sessionId": "결과 대화를 session_get으로 가져오기 위한 session ID"}, "guidance": "agent_run_status(runId)로 completed=true가 될 때까지 확인합니다."},
            {"name": "agent_run_status", "purpose": "실행 중 agent_run의 프로세스/I/O 상태 반환", "params": {"runId": "(선택) 실행 중 runId. 생략하면 전체 실행 중 run 목록 반환"}, "returns": {"running": "runId 지정 시 실행 중 여부", "completed": "완료 캐시에 있으면 true", "result": "완료 캐시에 있을 때 최종 결과(compiledPrompt 제외)", "count": "runId 생략 시 실행 중 건수", "runs": "실행 중 run의 elapsedSec, idleSec, stdoutLines, stderrLines 등", "recentCompleted": "최근 완료 run 요약 목록. stdout/stderr 본문은 포함하지 않음"}},
        ],
        "notes": [
            "role은 'user'와 'assistant'만 사용 가능합니다('system' 미지원).",
            "세션을 사용하면 과거 user/assistant 메시지가 자동으로 프롬프트에 포함됩니다.",
            "긴 프롬프트는 promptBase64로 Base64 인코딩해 전달할 수 있습니다.",
            f"타임아웃은 활동도 기반입니다. ORCH_IDLE_TIMEOUT_SEC(기본 {DEFAULT_IDLE_TIMEOUT_SEC}초) 동안 stdout/stderr 출력이 없으면 idle timeout으로 종료됩니다.",
            "timeoutMs는 절대 상한입니다.",
            "실행 중에는 30초마다 cli.alive 로그가 출력되어 경과 시간, idle 시간, 출력 행 수를 확인할 수 있습니다.",
            f"agent_run은 {AGENT_RUN_PROGRESS_INTERVAL_SEC}초마다 MCP progress heartbeat를 보내 장시간 실행 시 client 측 타임아웃을 줄입니다.",
            "agent_run_status는 프로세스와 stdout/stderr 활동만 반환하며 내부 모델 추론 상태는 직접 보여주지 않습니다.",
        ],
    }
    _log_tool_result("orchestrator_usage", {"tools": len(guide["tools"])})
    return guide

@mcp.tool(name="orchestrator_health", description="서버 상태와 DB 경로를 반환합니다.")
def orchestrator_health() -> dict[str, Any]:
    """서버 상태와 DB 경로를 반환합니다."""
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
    """세션을 생성합니다."""
    _log_tool_call("session_create", title=title)
    result = _get_store().create_session(title=title, messages=messages)
    _log_tool_result("session_create", result)
    return result


@mcp.tool(name="session_get", description="세션과 메시지 목록을 가져옵니다.")
def session_get(sessionId: str) -> dict[str, Any] | None:
    """세션과 메시지 이력을 조회합니다."""
    _log_tool_call("session_get", sessionId=sessionId)
    result = _get_store().get_session(sessionId)
    _log_tool_result("session_get", result)
    return result


@mcp.tool(name="session_list", description="최근 세션 목록을 가져옵니다.")
def session_list(limit: int = 20) -> list[dict[str, Any]]:
    """최근 세션 목록을 조회합니다."""
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
    """기존 세션에 메시지를 추가합니다."""
    _log_tool_call("session_append", sessionId=sessionId, agent=agent)
    result = _get_store().append_messages(session_id=sessionId, messages=messages, agent=agent)
    _log_tool_result("session_append", result)
    return result


@mcp.tool(name="session_delete", description="세션과 관련 메시지를 삭제합니다.")
def session_delete(sessionId: str) -> dict[str, Any]:
    """세션과 관련 메시지를 삭제합니다."""
    _log_tool_call("session_delete", sessionId=sessionId)
    result = _get_store().delete_session(sessionId)
    _log_tool_result("session_delete", result)
    return result


@mcp.tool(name="workflow_create", description="구조화된 워크플로 판단 로그를 시작합니다.")
def workflow_create(
    title: str | None = None,
    objective: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """workflow를 생성합니다."""
    _log_tool_call("workflow_create", title=title, objective=_truncate(objective or ""))
    result = _get_store().create_workflow(title=title, objective=objective, metadata=metadata)
    _log_tool_result("workflow_create", result)
    return result


@mcp.tool(name="workflow_decision_append", description="워크플로에 의사결정 로그를 추가합니다.")
def workflow_decision_append(
    workflowId: str,
    stage: str,
    role: str,
    agent: str | None = None,
    sourceRunId: str | None = None,
    sourceSessionId: str | None = None,
    expectedDecision: str | None = None,
    decision: str | None = None,
    summary: str | None = None,
    findings: list[dict[str, Any]] | None = None,
    nextAction: str | None = None,
    evidenceSummary: str | None = None,
    promptSummary: str | None = None,
    promptHash: str | None = None,
    status: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """workflow에 decision을 추가합니다."""
    _log_tool_call("workflow_decision_append", workflowId=workflowId, stage=stage, role=role, agent=agent)
    result = _get_store().append_workflow_decision(
        workflow_id=workflowId,
        stage=stage,
        role=role,
        agent=agent,
        source_run_id=sourceRunId,
        source_session_id=sourceSessionId,
        expected_decision=expectedDecision,
        decision=decision,
        summary=summary,
        findings=findings,
        next_action=nextAction,
        evidence_summary=evidenceSummary,
        prompt_summary=promptSummary,
        prompt_hash=promptHash,
        status=status,
        metadata=metadata,
    )
    _log_tool_result("workflow_decision_append", result)
    return result


@mcp.tool(name="workflow_get", description="워크플로와 의사결정 이력을 가져옵니다.")
def workflow_get(
    workflowId: str,
    includeDecisions: bool = True,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any] | None:
    """workflow 상세를 가져옵니다."""
    _log_tool_call("workflow_get", workflowId=workflowId, includeDecisions=includeDecisions, limit=limit, offset=offset)
    result = _get_store().get_workflow(
        workflowId,
        include_decisions=includeDecisions,
        limit=limit,
        offset=offset,
    )
    _log_tool_result("workflow_get", result)
    return result


@mcp.tool(name="workflow_list", description="최근 업데이트된 워크플로 목록을 가져옵니다.")
def workflow_list(status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """workflow 목록 조회 도구."""
    _log_tool_call("workflow_list", status=status, limit=limit)
    result = _get_store().list_workflows(status=status, limit=limit)
    _log_tool_result("workflow_list", result)
    return result


@mcp.tool(name="agent_run_status", description="실행 중 agent_run의 프로세스/I/O 상태를 반환합니다.")
def agent_run_status(runId: str | None = None) -> dict[str, Any]:
    """실행 중 agent_run 상태를 반환합니다."""
    _log_tool_call("agent_run_status", runId=runId)
    result = _snapshot_active_runs(runId)
    _log_tool_result("agent_run_status", result)
    return result


@mcp.tool(name="agent_run_start", description="Claude 또는 Codex CLI를 백그라운드에서 시작하고 runId를 즉시 반환합니다.")
def agent_run_start(
    agent: Literal["claude", "codex"],
    prompt: str = "",
    promptBase64: str | None = None,
    useSession: bool = True,
    sessionId: str | None = None,
    messages: list[dict[str, Any]] | None = None,
    filePaths: list[str] | None = None,
    allowedToolsPattern: str | None = None,
    skipPermissions: bool = False,
    codexMcpApprovedTools: list[str] | None = None,
    codexMcpApprovedWriteTools: list[str] | None = None,
    approveCodexMcpWrites: bool = False,
    cwd: str | None = None,
    timeoutMs: int | None = None,
    extraArgs: list[str] | None = None,
    workflow: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """비동기 agent 실행을 시작합니다."""
    resolved_prompt = _resolve_text_value(prompt, promptBase64, field_name="prompt")
    if agent not in {"claude", "codex"}:
        raise ValueError("agent must be claude or codex")
    if not resolved_prompt:
        raise ValueError("prompt is required")
    effective_skip_permissions = bool(skipPermissions)
    _log_tool_call(
        "agent_run_start",
        agent=agent,
        prompt=_truncate(resolved_prompt),
        useSession=useSession,
        sessionId=sessionId,
        filePaths=filePaths,
        allowedToolsPattern=allowedToolsPattern,
        skipPermissions=effective_skip_permissions,
        codexMcpApprovedTools=codexMcpApprovedTools,
        codexMcpApprovedWriteTools=codexMcpApprovedWriteTools,
        approveCodexMcpWrites=approveCodexMcpWrites,
        cwd=cwd,
        timeoutMs=timeoutMs,
        workflow=workflow,
    )
    user_extra_args: list[str] = list(extraArgs or [])
    _validate_extra_args(user_extra_args)
    effective_extra_args: list[str] = list(user_extra_args)
    if effective_skip_permissions and agent == "claude":
        effective_extra_args.append("--dangerously-skip-permissions")

    store = _get_store()
    if sessionId:
        if store.get_session(sessionId) is None:
            raise ValueError(f"session not found: {sessionId}")
        active_session_id = sessionId
    else:
        created = store.create_session(title=f"{agent}-{_now_iso()}", messages=None)
        active_session_id = created["session"]["id"]

    run_id = str(uuid4())
    effective_timeout_ms = int(timeoutMs or store.default_timeout_ms)
    _register_pending_agent_run(
        run_id=run_id,
        agent=agent,
        session_id=active_session_id,
        cwd=cwd,
        timeout_ms=effective_timeout_ms,
    )

    def _background_run() -> None:
        """백그라운드 스레드에서 실제 agent 실행과 완료 캐시 적재를 수행한다."""
        try:
            store.run_agent(
                agent=agent,
                prompt=resolved_prompt,
                use_session=useSession,
                session_id=active_session_id,
                messages=messages,
                file_paths=filePaths,
                allowed_tools_pattern=allowedToolsPattern,
                codex_mcp_approved_tools=codexMcpApprovedTools,
                codex_mcp_approved_write_tools=codexMcpApprovedWriteTools,
                approve_codex_mcp_writes=approveCodexMcpWrites,
                cwd=cwd,
                timeout_ms=timeoutMs,
                extra_args=effective_extra_args,
                workflow=workflow,
                run_id=run_id,
                _internal=effective_skip_permissions,
            )
            _forget_active_run(run_id)
        except Exception as exc:
            _forget_active_run(run_id)
            failure_payload = {
                "runId": run_id,
                "sessionId": active_session_id,
                "agent": agent,
                "exitCode": 1,
                "status": "failed",
                "stdout": "",
                "stderr": str(exc),
            }
            _remember_completed_run(run_id, failure_payload, error=str(exc))
            _log("agent_run_start.background_failed", level="ERROR", run_id=run_id, error=str(exc))

    thread = threading.Thread(target=_background_run, name=f"agent-run-start-{run_id}", daemon=True)
    thread.start()

    result = {
        "runId": run_id,
        "sessionId": active_session_id,
        "agent": agent,
        "status": "running",
        "background": True,
        "checkStatusTool": "agent_run_status",
        "guidance": "Poll agent_run_status(runId) until completed=true, then read result or use session_get(sessionId).",
    }
    _log_tool_result("agent_run_start", result)
    return result


# main agent run tool
@mcp.tool(name="agent_run", description="Claude 또는 Codex CLI를 실행하고 필요 시 세션에 저장합니다.")
async def agent_run(
    agent: Literal["claude", "codex"],
    prompt: str = "",
    promptBase64: str | None = None,
    useSession: bool = True,
    sessionId: str | None = None,
    messages: list[dict[str, Any]] | None = None,
    filePaths: list[str] | None = None,
    allowedToolsPattern: str | None = None,
    skipPermissions: bool = False,
    codexMcpApprovedTools: list[str] | None = None,
    codexMcpApprovedWriteTools: list[str] | None = None,
    approveCodexMcpWrites: bool = False,
    cwd: str | None = None,
    timeoutMs: int | None = None,
    extraArgs: list[str] | None = None,
    workflow: dict[str, Any] | None = None,
    *,
    ctx: Context,
) -> dict[str, Any]:
    """동기 대기형 agent 실행 도구. 장시간 실행 시 progress heartbeat를 보낸다."""
    resolved_prompt = _resolve_text_value(prompt, promptBase64, field_name="prompt")
    effective_skip_permissions = bool(skipPermissions)
    _log_tool_call(
        "agent_run",
        agent=agent,
        prompt=_truncate(resolved_prompt),
        useSession=useSession,
        sessionId=sessionId,
        filePaths=filePaths,
        allowedToolsPattern=allowedToolsPattern,
        skipPermissions=effective_skip_permissions,
        codexMcpApprovedTools=codexMcpApprovedTools,
        codexMcpApprovedWriteTools=codexMcpApprovedWriteTools,
        approveCodexMcpWrites=approveCodexMcpWrites,
        cwd=cwd,
        timeoutMs=timeoutMs,
        workflow=workflow,
    )
    user_extra_args: list[str] = list(extraArgs or [])
    _validate_extra_args(user_extra_args)
    effective_extra_args: list[str] = list(user_extra_args)
    if effective_skip_permissions and agent == "claude":
        effective_extra_args.append("--dangerously-skip-permissions")
    run_fn = functools.partial(
        _get_store().run_agent,
        agent=agent,
        prompt=resolved_prompt,
        use_session=useSession,
        session_id=sessionId,
        messages=messages,
        file_paths=filePaths,
        allowed_tools_pattern=allowedToolsPattern,
        codex_mcp_approved_tools=codexMcpApprovedTools,
        codex_mcp_approved_write_tools=codexMcpApprovedWriteTools,
        approve_codex_mcp_writes=approveCodexMcpWrites,
        cwd=cwd,
        timeout_ms=timeoutMs,
        extra_args=effective_extra_args,
        workflow=workflow,
        _internal=effective_skip_permissions,
    )
    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, run_fn)
    elapsed_sec = 0
    while True:
        done, _pending = await asyncio.wait({task}, timeout=AGENT_RUN_PROGRESS_INTERVAL_SEC)
        if task in done:
            break
        elapsed_sec += AGENT_RUN_PROGRESS_INTERVAL_SEC
        try:
            await ctx.report_progress(progress=float(elapsed_sec), total=0.0)
            _log("agent_run.progress_heartbeat", elapsed_sec=elapsed_sec)
        except Exception as exc:
            _log("agent_run.progress_heartbeat_failed", elapsed_sec=elapsed_sec, error=str(exc))
    result = task.result()
    _log_tool_result("agent_run", result)
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """MCP 서버 프로세스 진입점을 실행합니다."""
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
