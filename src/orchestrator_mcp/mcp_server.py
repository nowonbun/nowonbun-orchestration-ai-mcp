from __future__ import annotations

import json
import sys
from typing import Any


def _as_text_result(value: Any) -> dict:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(value, ensure_ascii=False, indent=2),
            }
        ]
    }


class McpServer:
    def __init__(self, *, name: str, version: str, store, sse_server) -> None:
        self.name = name
        self.version = version
        self.store = store
        self.sse_server = sse_server

    def serve_forever(self) -> None:
        stdin = sys.stdin.buffer
        while True:
            try:
                headers = self._read_headers(stdin)
            except ValueError as exc:
                self._log(f"invalid header: {exc}")
                continue
            if headers is None:
                return

            try:
                content_length = int(headers.get("content-length", "0"))
            except ValueError:
                self._log(f"invalid content-length: {headers.get('content-length')}")
                continue

            if content_length <= 0:
                self._log("missing or non-positive content-length")
                continue

            body = stdin.read(content_length)
            if not body:
                return

            try:
                message = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError as exc:
                self._log(f"invalid json payload: {exc}")
                continue

            if "id" not in message:
                self._log(f"notification ignored: {message.get('method')}")
                continue
            try:
                result = self._route(message.get("method"), message.get("params") or {})
                self._write_message({"jsonrpc": "2.0", "id": message["id"], "result": result})
            except Exception as exc:
                self._write_message(
                    {
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "error": {"code": -32000, "message": str(exc)},
                    }
                )

    def _read_headers(self, stream) -> dict[str, str] | None:
        headers: dict[str, str] = {}
        while True:
            line = stream.readline()
            if not line:
                return None
            if line == b"\r\n":
                return headers
            decoded = line.decode("utf-8").strip()
            if ":" not in decoded:
                raise ValueError(decoded)
            key, value = decoded.split(":", 1)
            headers[key.strip().lower()] = value.strip()

    def _write_message(self, payload: dict) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(encoded)}\r\n\r\n".encode("ascii"))
        sys.stdout.buffer.write(encoded)
        sys.stdout.buffer.flush()

    def _route(self, method: str, params: dict) -> dict:
        if method == "initialize":
            return {
                "protocolVersion": params.get("protocolVersion", "2025-03-26"),
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": self.name, "version": self.version},
            }
        if method == "initialized":
            return {}
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": self._get_tools()}
        if method == "tools/call":
            return self._call_tool(params)
        raise ValueError(f"unsupported method: {method}")

    @staticmethod
    def _log(message: str) -> None:
        print(f"[mcp-server] {message}", file=sys.stderr, flush=True)

    def _get_tools(self) -> list[dict]:
        return [
            {
                "name": "orchestrator_health",
                "description": "오케스트레이터 상태, DB 경로, SSE 주소, 캐시 상태를 반환합니다.",
                "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}},
            },
            {
                "name": "session_create",
                "description": "세션을 생성하고 초기 메시지를 저장합니다.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"title": {"type": "string"}, "messages": {"type": "array"}, "metadata": {"type": "object"}},
                },
            },
            {
                "name": "session_get",
                "description": "세션과 메시지 목록을 조회합니다.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["sessionId"],
                    "properties": {"sessionId": {"type": "string"}},
                },
            },
            {
                "name": "session_list",
                "description": "최근 세션 목록을 조회합니다.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"limit": {"type": "number"}},
                },
            },
            {
                "name": "session_append",
                "description": "기존 세션에 메시지를 추가합니다.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["sessionId", "messages"],
                    "properties": {
                        "sessionId": {"type": "string"},
                        "messages": {"type": "array"},
                        "agent": {"type": "string"},
                        "metadata": {"type": "object"},
                    },
                },
            },
            {
                "name": "session_delete",
                "description": "세션과 연결 메시지를 삭제합니다.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["sessionId"],
                    "properties": {"sessionId": {"type": "string"}},
                },
            },
            {
                "name": "agent_run",
                "description": "Claude 또는 Codex CLI를 실행하고 선택적으로 세션 문맥을 저장합니다.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["agent", "prompt"],
                    "properties": {
                        "agent": {"type": "string", "enum": ["claude", "codex"]},
                        "prompt": {"type": "string"},
                        "systemPrompt": {"type": "string"},
                        "useSession": {"type": "boolean"},
                        "sessionId": {"type": "string"},
                        "messages": {"type": "array"},
                        "allowedToolsPattern": {"type": "string"},
                        "cwd": {"type": "string"},
                        "timeoutMs": {"type": "number"},
                        "extraArgs": {"type": "array"},
                    },
                },
            },
        ]

    def _call_tool(self, params: dict) -> dict:
        name = params.get("name")
        arguments = params.get("arguments") or {}

        if name == "orchestrator_health":
            return _as_text_result(
                {
                    **self.store.get_health(),
                    "sse": {
                        "host": self.sse_server.host,
                        "port": self.sse_server.port,
                        "eventsUrl": f"http://{self.sse_server.host}:{self.sse_server.port}/events",
                    },
                }
            )
        if name == "session_create":
            return _as_text_result(self.store.create_session(arguments.get("title"), arguments.get("messages"), arguments.get("metadata")))
        if name == "session_get":
            return _as_text_result(self.store.get_session(arguments["sessionId"]))
        if name == "session_list":
            return _as_text_result(self.store.list_sessions(arguments.get("limit", 20)))
        if name == "session_append":
            return _as_text_result(self.store.append_messages(arguments["sessionId"], arguments["messages"], arguments.get("agent"), arguments.get("metadata")))
        if name == "session_delete":
            return _as_text_result(self.store.delete_session(arguments["sessionId"]))
        if name == "agent_run":
            return _as_text_result(
                self.store.run_agent(
                    agent=arguments["agent"],
                    prompt=arguments["prompt"],
                    system_prompt=arguments.get("systemPrompt"),
                    use_session=arguments.get("useSession", True),
                    session_id=arguments.get("sessionId"),
                    messages=arguments.get("messages"),
                    allowed_tools_pattern=arguments.get("allowedToolsPattern", "mcp__*"),
                    cwd=arguments.get("cwd"),
                    timeout_ms=arguments.get("timeoutMs"),
                    extra_args=arguments.get("extraArgs"),
                )
            )
        raise ValueError(f"unknown tool: {name}")
