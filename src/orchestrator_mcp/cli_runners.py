from __future__ import annotations

import subprocess
import threading
from pathlib import Path
from typing import Callable


def _build_command(agent: str, prompt: str, allowed_tools_pattern: str | None, extra_args: list[str] | None) -> list[str]:
    args = extra_args or []

    if agent == "claude":
        command = ["claude", "-p", prompt]
        if allowed_tools_pattern:
            command.extend(["--allowedTools", allowed_tools_pattern])
        command.extend(args)
        return command

    if agent == "codex":
        command = ["codex", "exec", prompt]
        command.extend(args)
        return command

    raise ValueError(f"unsupported agent: {agent}")


def run_agent_cli(
    *,
    agent: str,
    prompt: str,
    cwd: str | None,
    timeout_ms: int,
    allowed_tools_pattern: str | None,
    extra_args: list[str] | None,
    on_stdout: Callable[[str], None] | None = None,
    on_stderr: Callable[[str], None] | None = None,
) -> dict:
    command = _build_command(agent, prompt, allowed_tools_pattern, extra_args)
    working_directory = str(Path(cwd).resolve()) if cwd else None

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

    def consume(stream, buffer: list[str], callback: Callable[[str], None] | None) -> None:
        try:
            for chunk in iter(stream.readline, ""):
                buffer.append(chunk)
                if callback:
                    callback(chunk)
        finally:
            stream.close()

    stdout_thread = threading.Thread(target=consume, args=(process.stdout, stdout_chunks, on_stdout), daemon=True)
    stderr_thread = threading.Thread(target=consume, args=(process.stderr, stderr_chunks, on_stderr), daemon=True)
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
        raise TimeoutError(f"agent command timed out after {timeout_ms} ms") from exc
    finally:
        if not timed_out:
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

    return {
        "stdout": "".join(stdout_chunks).strip(),
        "stderr": "".join(stderr_chunks).strip(),
        "exitCode": int(exit_code),
    }
