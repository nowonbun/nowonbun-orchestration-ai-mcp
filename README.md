# nowonbun-orchestration-ai-mcp

FastMCP 기반으로 Claude CLI와 Codex CLI를 하나의 MCP 서버에서 실행하고, 세션 메시지를 SQLite에 단순하게 저장하는 Python 서버입니다.

## 구조
- 런타임 코드는 `src/server.py` 한 파일입니다.
- MCP 서버 구현은 커스텀 JSON-RPC 루프가 아니라 `mcp.server.fastmcp.FastMCP`를 사용합니다.
- DB는 `sessions`, `messages` 두 테이블만 사용합니다.
- `messages`는 `session_id`, `role`, `content`, `agent`, `created_at`, `sort_order`, `is_session`을 저장합니다.

## 요구 사항
- Python 3.13 이상
- `mcp[cli]` 1.26 이상, 2 미만
- 전역 설치된 CLI
  - `claude`
  - `codex`

## 실행
```bash
set PYTHONPATH=D:\work\nowonbun-orchestration-ai-mcp\src
set ORCH_TRANSPORT=streamable-http
set ORCH_HOST=127.0.0.1
set ORCH_PORT=18282
python D:\work\nowonbun-orchestration-ai-mcp\src\server.py
```

기본 실행 transport는 `streamable-http`입니다.
기본 MCP 엔드포인트는 `http://127.0.0.1:18282/mcp`입니다.

## 환경 변수
- `ORCH_DB_PATH`: SQLite 파일 경로, 기본값 `./data/orchestrator.sqlite`
- `ORCH_TRANSPORT`: `stdio`, `sse`, `streamable-http` 중 하나, 기본값 `streamable-http`
- `ORCH_HOST`: HTTP 계열 transport host, 기본값 `127.0.0.1`
- `ORCH_PORT`: HTTP 계열 transport port, 기본값 `18282`
- `ORCH_DEFAULT_TIMEOUT_MS`: 기본값 `120000`

서버형 실행 예시:
```bash
set ORCH_TRANSPORT=streamable-http
set ORCH_HOST=127.0.0.1
set ORCH_PORT=18282
python D:\work\nowonbun-orchestration-ai-mcp\src\server.py
```

stdio 실행 예시:
```bash
set ORCH_TRANSPORT=stdio
python D:\work\nowonbun-orchestration-ai-mcp\src\server.py
```

## MCP 도구

### `orchestrator_health`
서버 상태, DB 경로, transport, host, port를 반환합니다.

### `session_create`
세션을 만들고 초기 메시지를 저장합니다.

예시:
```json
{
  "title": "spring-study",
  "messages": [
    { "role": "system", "content": "너는 개발 도우미다." },
    { "role": "user", "content": "Spring Boot 설명해줘" }
  ]
}
```

### `session_get`
세션과 메시지 목록을 조회합니다.

### `session_list`
최근 세션 목록을 조회합니다.

### `session_append`
기존 세션에 메시지를 추가합니다.

### `session_delete`
세션과 메시지를 삭제합니다.

### `agent_run`
Claude 또는 Codex를 실행합니다.

예시:
```json
{
  "agent": "claude",
  "useSession": true,
  "sessionId": "세션ID",
  "systemPrompt": "너는 개발 도우미다.",
  "prompt": "JPA 설명해줘",
  "allowedToolsPattern": "mcp__*",
  "cwd": "D:/work",
  "timeoutMs": 120000
}
```

## 세션 저장 방식
- `useSession: true`
  - `sessionId`가 있으면 기존 세션을 사용합니다.
  - `sessionId`가 없으면 새 세션을 자동 생성합니다.
  - 요청 메시지와 응답 메시지를 모두 DB에 저장합니다.
  - 다음 호출 시 `is_session = 1` 메시지만 이어서 프롬프트를 구성합니다.
- `useSession: false`
  - 요청 메시지와 응답 메시지를 모두 DB에 저장합니다.
  - 단, 이번 호출에서 저장한 `is_session = 0` 메시지는 다음 호출 세션 문맥에 재사용하지 않습니다.

## messages 컬럼 의미
- `role`: `system`, `user`, `assistant`, `tool`
- `agent`: 발화를 처리한 에이전트 이름입니다. `claude`, `codex`처럼 저장합니다.
- 사용자 입력 행은 `role = "user"`로 저장되고, `agent`는 해당 호출을 처리한 에이전트명으로 저장됩니다.
- 모델 응답 행은 `role = "assistant"`로 저장되고, `agent`는 해당 응답을 만든 에이전트명으로 저장됩니다.
- 수동으로 생성한 초기 세션 메시지는 에이전트가 정해지지 않았으면 `agent = null`일 수 있습니다.

## Transport
- `streamable-http`: 기본 서버 운영 방식, 기본 엔드포인트는 `http://127.0.0.1:18282/mcp`
- `stdio`: Codex/Claude가 프로세스를 직접 붙는 방식
- `sse`: FastMCP SSE transport
- `streamable-http`: FastMCP Streamable HTTP transport

## Codex 등록 예시 (`config.toml`)
아래 형식은 `C:\Users\nowonbun\.codex\config.toml`의 실제 MCP 등록 형식인 `[mcp_servers.<name>]` 구조를 기준으로 작성했습니다.

```toml
[mcp_servers.nowonbun_orchestration]
url = "http://127.0.0.1:18282/mcp"
tool_timeout_sec = 300
```

로컬 프로세스를 직접 붙는 stdio 등록이 필요하면:

```toml
[mcp_servers.nowonbun_orchestration]
command = "python"
args = ["D:/work/nowonbun-orchestration-ai-mcp/src/server.py"]
tool_timeout_sec = 300

[mcp_servers.nowonbun_orchestration.env]
ORCH_TRANSPORT = "stdio"
PYTHONPATH = "D:/work/nowonbun-orchestration-ai-mcp/src"
```

## 참고
- 기존 커스텀 MCP JSON-RPC 루프와 커스텀 SSE 서버는 제거했습니다.
- FastMCP API는 공식 `modelcontextprotocol/python-sdk` README의 v1.x FastMCP 예제를 기준으로 맞췄습니다.
- 메시지 순서는 `sort_order` 컬럼으로 관리하고 API 응답에는 `order`로 반환합니다.

## Claude ?? ?? (`mcpServers` JSON)
?? ??? Claude Desktop / Claude Code?? ?? ???? `mcpServers` JSON ?? ?????. ???? ??: ? ??? ?? ?? Claude ?? ??? ?? ??? ??? ??, ? ??? ?? JSON ??? Claude? ?? ???? ??? ? ????.

```json
{
  "mcpServers": {
    "nowonbun_orchestration": {
      "command": "python",
      "args": ["D:/work/nowonbun-orchestration-ai-mcp/src/server.py"],
      "env": {
        "ORCH_TRANSPORT": "stdio",
        "PYTHONPATH": "D:/work/nowonbun-orchestration-ai-mcp/src"
      }
    }
  }
}
```

? ??? ?? URL? ?? ??? ???, Claude? ??? ? ?? ????? ?? ???? ?????.
