# nowonbun-orchestration-ai-mcp

Claude CLI와 Codex CLI를 하나의 MCP 서버 뒤에서 호출하고, 세션 문맥을 SQLite와 메모리 캐시에 함께 유지하는 Python 오케스트레이션 서버입니다.

## 제공 기능
- `claude -p`, `codex exec` 래핑
- 메시지 배열 기반 세션 문맥 누적
- 함수별 `useSession` 사용/미사용 분리
- 모든 실행 내역을 `data/orchestrator.sqlite`에 저장
- 서버 실행 중 최근 세션을 메모리 캐시에 유지
- SSE(`/events`)로 실행 진행 상황과 스트림 조각 브로드캐스트
- MCP stdio 서버로 Claude/Codex에서 직접 연결 가능

## 요구 사항
- Python 3.13 이상
- 전역 설치된 CLI
  - `claude`
  - `codex`

## 빠른 시작
```bash
python -m orchestrator_mcp
```

또는:
```bash
set PYTHONPATH=D:\work\nowonbun-orchestration-ai-mcp\src
python -m orchestrator_mcp
```

기본 SSE 서버:
- `http://127.0.0.1:8765/health`
- `http://127.0.0.1:8765/events`

## 환경 변수
- `ORCH_DB_PATH`: SQLite 파일 경로, 기본값 `./data/orchestrator.sqlite`
- `ORCH_SSE_HOST`: SSE 바인딩 호스트, 기본값 `127.0.0.1`
- `ORCH_SSE_PORT`: SSE 포트, 기본값 `8765`
- `ORCH_CACHE_LIMIT`: 메모리 세션 캐시 개수, 기본값 `100`
- `ORCH_DEFAULT_TIMEOUT_MS`: CLI 실행 제한 시간, 기본값 `120000`

## MCP 도구

### 1. `orchestrator_health`
서버 상태, DB 경로, SSE 주소, 캐시 상태를 반환합니다.

### 2. `session_create`
세션을 생성하고 초기 메시지를 저장합니다.

입력 예시:
```json
{
  "title": "spring-study",
  "messages": [
    { "role": "system", "content": "너는 친절한 개발 도우미다." },
    { "role": "user", "content": "Spring Boot 설명해줘" }
  ],
  "metadata": {
    "topic": "backend"
  }
}
```

### 3. `session_get`
세션과 메시지 목록을 조회합니다.

### 4. `session_list`
최근 세션 목록을 조회합니다.

### 5. `session_append`
기존 세션에 메시지를 추가합니다.

### 6. `session_delete`
세션과 연결 메시지를 삭제합니다.

### 7. `agent_run`
Claude 또는 Codex를 실행합니다.

입력 예시:
```json
{
  "agent": "claude",
  "useSession": true,
  "sessionId": "세션ID",
  "systemPrompt": "너는 친절한 개발 도우미다.",
  "prompt": "JPA도 설명해줘",
  "allowedToolsPattern": "mcp__*",
  "cwd": "D:/work",
  "timeoutMs": 120000
}
```

메시지 배열만으로 실행하고 싶으면:
```json
{
  "agent": "codex",
  "useSession": false,
  "messages": [
    { "role": "system", "content": "너는 친절한 개발 도우미다." },
    { "role": "user", "content": "Spring Boot 설명해줘" },
    { "role": "assistant", "content": "Spring Boot는..." },
    { "role": "user", "content": "JPA도 설명해줘" }
  ],
  "prompt": "이 대화 기준으로 JPA 핵심만 다시 정리해줘"
}
```

## 세션 동작 방식
- `useSession: true`
  - `sessionId`가 있으면 기존 세션을 불러옵니다.
  - `sessionId`가 없으면 새 세션을 자동 생성합니다.
  - 실행 전 사용자 메시지와 실행 후 assistant 응답이 세션에 반영됩니다.
- `useSession: false`
  - DB에는 실행 로그가 저장되지만 세션 문맥은 재사용하지 않습니다.

## SSE 이벤트
- `server.ready`
- `run.started`
- `run.stdout`
- `run.stderr`
- `run.completed`
- `run.failed`
- `session.updated`

예시:
```bash
curl -N http://127.0.0.1:8765/events
```

## Claude Desktop / Claude Code 예시
실사용 시 MCP 클라이언트 설정에 stdio 명령으로 등록합니다.

```json
{
  "mcpServers": {
    "orchestrator": {
      "command": "python",
      "args": ["-m", "orchestrator_mcp"],
      "env": {
        "PYTHONPATH": "D:/work/nowonbun-orchestration-ai-mcp/src"
      }
    }
  }
}
```

## Codex 예시
Codex에서 MCP 서버를 외부 프로세스로 등록한 뒤 `mcp__orchestrator__agent_run` 같은 도구로 호출하면 됩니다.

## 저장 구조
- `sessions`: 세션 메타데이터
- `messages`: 세션 메시지
- `runs`: CLI 실행 로그

## 제한 사항
- Claude/Codex CLI 자체의 내부 세션 ID를 재사용하는 방식은 아닙니다.
- 이 서버가 대화 기록을 재구성해서 매 실행마다 CLI에 전달합니다.
- CLI 옵션이 바뀌면 `src/orchestrator_mcp/cli_runners.py`를 맞춰야 합니다.
