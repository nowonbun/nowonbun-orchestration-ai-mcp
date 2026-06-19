---
name: mcp-orchestration-ai
description: mcp-orchestration-ai를 통해 Claude 또는 Codex CLI를 실행하는 담당자는 세션 관리, cwd 제어, 실행 결과 검증을 이 절차로 수행해야 합니다.
---

# MCP Orchestration AI Skill

# Must

## Scope
- 이 스킬은 `mcp-orchestration-ai` MCP 서버를 통해 Claude 또는 Codex CLI를 실행할 때 반드시 적용합니다.
- 이 스킬은 `orchestrator_health`, `orchestrator_usage`, `session_create`, `session_get`, `session_list`, `session_append`, `session_delete`, `agent_run` 호출 판단에 적용합니다.
- 이 스킬은 MCP 서버 구현 변경이 아니라 MCP 도구 사용 시 입력, 실행, 검증, 보고를 규정합니다.

## Source of Truth
- `src/server.py`는 MCP 도구 정의, 매개변수 이름, CLI 실행 방식, 차단 대상 `extraArgs`, 기본 `cwd` 해석 기준입니다.
- `orchestrator_usage`는 실행 중 MCP 서버가 반환하는 사용 가이드의 기준입니다.
- `orchestrator_health`는 실행 중 MCP 서버의 연결 상태, DB 경로, transport, host, port 기준입니다.
- 이 `SKILL.md`는 MCP 사용 절차, 입력 검증, 결과 검증, 보고 항목의 기준입니다.

## Tool Selection
- MCP 서버 생존 확인이 필요할 때는 `orchestrator_health`를 반드시 사용합니다.
- 실행 중 서버의 도구 사양을 확인할 때는 `orchestrator_usage`를 반드시 사용합니다.
- 초기 메시지를 미리 설정한 세션이 필요할 때는 `session_create`를 반드시 사용합니다.
- Claude 또는 Codex CLI를 실행할 때는 `agent_run`을 반드시 사용합니다.
- 기존 세션의 기록을 확인할 때는 `session_get`을 반드시 사용합니다.
- 최근 세션 후보를 확인할 때는 `session_list`를 반드시 사용합니다.
- CLI를 실행하지 않고 기록만 추가할 때는 `session_append`를 반드시 사용합니다.
- 세션 삭제가 명시적으로 요청된 경우에만 `session_delete`를 사용합니다.

## Input Rules
- `agent_run.agent`는 `claude` 또는 `codex` 중 하나로 고정합니다.
- `agent_run.prompt` 또는 `agent_run.promptBase64` 중 최소 하나를 지정합니다.
- 장문, 줄바꿈, 인용, JSON, 비 ASCII 문자를 포함한 프롬프트에서 인수 길이 또는 이스케이프 손상 위험이 있을 때는 `promptBase64`를 사용합니다.
- 대화를 이어가야 할 때는 이전 응답의 `sessionId`를 `agent_run.sessionId`에 지정합니다.
- 대화 기록을 사용하지 않는 단발 실행에서는 `useSession`을 `false`로 설정합니다.
- 작업 디렉터리를 고정해야 할 때는 `cwd`에 절대 경로를 지정합니다.
- `cwd`를 생략했을 때는 MCP 서버 구현의 기본 base directory에서 CLI가 실행되는 것으로 간주합니다.
- `messages`를 지정할 때는 각 요소의 `role`을 `user` 또는 `assistant`로 설정합니다.
- `messages`를 지정할 때는 각 요소에 `content`를 포함합니다.
- `timeoutMs`는 실행 내용이 기본 타임아웃을 초과할 것으로 예상될 때 명시합니다.
- `allowedToolsPattern`은 Claude CLI에 허용 도구 제약을 전달해야 할 때만 명시합니다.

## Execution Rules
- `agent_run` 실행 전에 필요한 `agent`, `prompt`, `cwd`, `sessionId`, `timeoutMs`의 정합성을 확인합니다.
- `cwd`에 상대 경로를 전달할 경우에는 호출 측에서 절대 경로로 해석 가능함을 확인한 뒤 사용합니다.
- 현재 base directory 또는 cwd를 확인할 때는 소스 파일이나 기존 세션 추정이 아니라 `agent_run`으로 대상 agent에 직접 질의합니다.
- Claude와 Codex를 모두 비교할 때는 동일 조건의 `agent_run`을 `agent=claude`와 `agent=codex`에 대해 각각 실행합니다.
- `sessionId`를 지정해 이어갈 경우에는 대상 agent와 대화 목적이 기존 세션과 일치하는지 확인합니다.
- 파괴적 작업, 외부 전송, 인증 정보 참조, 공유 상태 변경을 위임할 경우에는 상위 정책이 요구하는 승인 조건을 충족하기 전까지 실행하지 않습니다.

## Result Handling
- `agent_run.exitCode`가 `0`일 때만 CLI 실행 성공으로 간주합니다.
- `agent_run.status`가 `completed`가 아니면 실패로 간주합니다.
- `stdout`는 agent의 응답 본문으로 간주합니다.
- `stderr`는 실행 로그, 경고, 에러의 증거로 확인합니다.
- `sessionId`는 연속 실행, 감사, 기록 확인에 사용할 수 있는 식별자로 저장하거나 보고합니다.
- `runId`가 반환된 경우에는 단일 실행 식별자로 보고합니다.

## Reporting
- `agent_run` 실행 후에는 `agent`, `runId`, `sessionId`, `exitCode`, `status`를 보고합니다.
- `cwd` 또는 base directory를 확인한 경우에는 질의에 사용한 `agent_run`의 `stdout`를 근거로 보고합니다.
- 실패 시에는 `stderr` 또는 MCP 응답의 실패 필드를 근거로 보고합니다.
- 세션 삭제를 실행한 경우에는 대상 `sessionId`, 삭제 결과, 재실행 필요 여부를 보고합니다.

# Must NOT

- `agent_run.extraArgs`에 `-p`를 지정하면 안 됩니다.
- `agent_run.extraArgs`에 `--print`를 지정하면 안 됩니다.
- `agent_run.extraArgs`에 `--allowedTools` 또는 `--allowed-tools`를 지정하면 안 됩니다.
- `agent_run.extraArgs`에 `--dangerously-skip-permissions`를 지정하면 안 됩니다.
- `messages.role`에 `system`을 지정하면 안 됩니다.
- 기존 세션의 과거 응답만을 근거로 현재 `agent_run`의 cwd를 단정하면 안 됩니다.
- `exitCode` 또는 `status`를 확인하지 않고 `agent_run`의 성공을 보고하면 안 됩니다.
- 사용자가 명시하지 않은 세션을 삭제하면 안 됩니다.
- 인증 정보, 토큰, API key, 개인정보를 `prompt`, `messages`, `session_append`에 포함하면 안 됩니다.

# Flow

1. `orchestrator_health`로 MCP 서버 연결 상태를 확인합니다.
2. 도구 사양이나 매개변수 판단이 불명확한 경우 `orchestrator_usage`를 확인합니다.
3. 실행 대상 agent, prompt, session 사용 여부, cwd, timeout을 결정합니다.
4. `agent_run`을 실행합니다.
5. 응답의 `status`, `exitCode`, `stdout`, `stderr`, `sessionId`를 확인합니다.
6. 연속 대화가 필요하면 이전 응답의 `sessionId`를 다음 `agent_run`에 전달합니다.
7. 기록 감사가 필요하면 `session_get`으로 대상 `sessionId`를 확인합니다.
8. 실행 결과와 근거를 `Reporting` 규칙에 따라 보고합니다.

# Examples

## Single Run Without Session
```json
{
  "agent": "claude",
  "prompt": "Python으로 소수 판별 함수를 작성해 주세요.",
  "useSession": false,
  "timeoutMs": 120000
}
```

## Continue Existing Session
```json
{
  "agent": "claude",
  "prompt": "이전 구현에 에러 핸들링을 추가해 주세요.",
  "sessionId": "<previous-session-id>",
  "useSession": true
}
```

## Confirm Current cwd Through agent_run
```json
{
  "agent": "codex",
  "prompt": "현재 작업 디렉터리를 한 줄만 출력해 주세요.",
  "useSession": false,
  "timeoutMs": 120000
}
```

## Run In Explicit Directory
```json
{
  "agent": "codex",
  "prompt": "이 저장소의 테스트 방침을 요약해 주세요.",
  "cwd": "/Users/soonyub.hwang/Works/github/lsm",
  "useSession": false
}
```

# Definition of Done

## Verification
- frontmatter에는 `name`과 `description`만 존재합니다.
- `# Must`, `# Must NOT`, `# Flow`, `# Examples`, `# Definition of Done`가 존재합니다.
- `# Must` 규칙은 MCP 사용 범위, 입력, 실행, 결과 처리, 보고를 분리합니다.
- `# Must NOT` 규칙은 차단 대상 인수, 금지 role, 미검증 단정, 미승인 삭제, 비밀 정보 혼입을 금지합니다.
- `agent_run` 사용 예시는 단발 실행, 연속 실행, cwd 확인, 명시적 cwd 실행을 포함합니다.
- `src/server.py`와 `orchestrator_usage`의 도구명, 주요 매개변수, 반환값과 모순되지 않습니다.
- Markdown는 UTF-8로 읽을 수 있습니다.
