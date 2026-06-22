---
name: mcp-orchestration-ai
description: mcp-orchestration-ai를 통해 Claude 또는 Codex CLI를 실행하는 담당자는 세션 관리, 비동기 실행 폴링, 워크플로우 판단 로그 검증을 이 절차에 따라 실시해야 합니다.
---

# MCP Orchestration AI Skill

# Must

## Scope
- 이 스킬은 `mcp-orchestration-ai` MCP 서버를 통해 Claude 또는 Codex CLI를 실행할 때 반드시 적용합니다.
- 이 스킬은 `orchestrator_health`, `orchestrator_usage`, `session_create`, `session_get`, `session_list`, `session_append`, `session_delete`, `agent_run`, `agent_run_start`, `agent_run_status`, `workflow_create`, `workflow_decision_append`, `workflow_get`, `workflow_list` 호출 판단에 적용합니다.
- 이 스킬은 MCP 서버의 구현 변경이 아닌, MCP 도구 사용 시의 입력, 실행, 검증, 보고를 규정합니다.

## Source of Truth
- 실행 중인 MCP 서버의 `orchestrator_usage`는 사용 가능한 도구, 주요 파라미터, 반환값, 장시간 실행 방침을 판단하는 기준입니다.
- 실행 중인 MCP 서버의 `orchestrator_health`는 연결 상태, DB 경로, transport, host, port, 기본 timeout을 판단하는 기준입니다.
- `src/server.py`는 구현 상의 MCP 도구 정의, 파라미터 명, CLI 실행 방식, `extraArgs` 블록 대상, workflow 저장 사양을 판단하는 기준입니다.
- 이 `SKILL.md`는 사용자가 MCP 도구를 구분하여 호출하고, 결과를 검증하고, 보고하는 절차를 판단하는 기준입니다.
- 실행 중인 MCP 서버의 `orchestrator_usage`와 로컬 `src/server.py`가 상충하는 경우, 운영 중인 호출 판단에서는 `orchestrator_usage`를 우선합니다.

## Tool Selection
- MCP 서버의 생존 확인이 필요할 때는 `orchestrator_health`를 반드시 사용합니다.
- 실행 중인 서버의 도구 사양을 확인할 때는 `orchestrator_usage`를 반드시 사용합니다.
- 초기 메시지를 사전 설정한 세션이 필요할 때는 `session_create`를 사용합니다.
- 기존 세션의 이력을 확인할 때는 `session_get`을 사용합니다.
- 최근 세션 후보를 확인할 때는 `session_list`를 사용합니다.
- CLI를 실행하지 않고 이력만 추가할 때는 `session_append`를 사용합니다.
- 세션 삭제가 명시적으로 요청된 경우에만 `session_delete`를 사용합니다.
- 짧은 동기 실행으로, 호출 측이 최종 결과를 기다릴 수 있다고 판단될 때는 `agent_run`을 사용할 수 있습니다.
- 배치 실행, 실행 시간이 불명확한 실행, 120초를 초과할 가능성이 있는 실행, 또는 호출 측 timeout을 피해야 하는 실행에는 `agent_run_start`를 사용합니다.
- `agent_run_start`로 시작한 실행의 완료 확인에는 동일한 `runId`를 지정하여 `agent_run_status`를 사용합니다.
- 실행 중인 run의 목록 또는 최근 완료 run의 요약을 확인할 때는 `runId`를 생략하고 `agent_run_status`를 사용합니다.
- 계획, 리뷰, 실행, 재리뷰 등의 판단 로그를 구조화하여 축적할 때는 `workflow_create`를 사용합니다.
- workflow에 stage/role 단위의 판단을 추가할 때는 `workflow_decision_append`를 사용합니다.
- workflow와 decision 이력을 확인할 때는 `workflow_get`을 사용합니다.
- 최근 업데이트된 workflow 후보를 확인할 때는 `workflow_list`를 사용합니다.

## Input Rules
- `agent_run.agent`와 `agent_run_start.agent`는 `claude` 또는 `codex` 중 하나로 고정합니다.
- `agent_run.prompt`, `agent_run.promptBase64`, `agent_run_start.prompt`, `agent_run_start.promptBase64` 각 실행에서는 일반 텍스트 또는 Base64 중 하나로 prompt를 지정합니다.
- 장문, 줄바꿈, 인용, JSON, 비 ASCII 문자를 포함하는 prompt에서 인수 길이 또는 이스케이프 손상의 위험이 있을 때는 `promptBase64`를 사용합니다.
- 대화 계속이 필요할 때는 이전 응답의 `sessionId`를 다음 실행의 `sessionId`에 지정합니다.
- 대화 이력을 사용하지 않는 단발 실행에서는 `useSession`을 `false`로 설정합니다.
- 작업 디렉터리를 고정해야 할 때는 `cwd`에 절대 경로를 지정합니다.
- `cwd`를 생략했을 때는 MCP 서버 구현의 기본 base directory에서 CLI가 실행되는 것으로 취급합니다.
- `messages`를 지정할 때는 각 요소의 `role`을 `user` 또는 `assistant`로 설정합니다.
- `messages`를 지정할 때는 각 요소에 `content`를 포함합니다.
- `timeoutMs`는 하드 타임아웃으로 취급하고, 장시간 실행에서는 예상 최대 시간보다 큰 값을 지정합니다.
- `allowedToolsPattern`은 Claude CLI에 허용 도구 제약을 전달해야 할 때만 지정합니다.
- `skipPermissions`는 상위 정책에서 허용되고, 비대화형 실행에서 Claude의 권한 확인을 우회해야 할 때만 지정합니다.
- Codex의 MCP 도구 사전 승인이 필요할 때는 `codexMcpApprovedTools`를 지정합니다.
- Codex의 MCP write 도구 사전 승인이 필요할 때는 `codexMcpApprovedWriteTools`와 `approveCodexMcpWrites=true`를 동시에 지정합니다.
- `agent_run.workflow` 또는 `agent_run_start.workflow`를 지정할 때는 기존 workflow의 `id`, `stage`, `role`, 판단 요약, 근거 요약을 raw prompt 없이 전달합니다.
- `agent_run_status.runId`는 단일 run의 상태 또는 완료 결과를 확인할 때 지정합니다.
- `workflow_create`에서는 `title`, `objective`, `metadata`를 raw prompt 없이 지정합니다.
- `workflow_decision_append`에서는 `workflowId`, `stage`, `role`을 반드시 지정합니다.
- `workflow_get`에서는 `workflowId`를 반드시 지정합니다.
- `workflow_list`에서는 필요한 경우에만 `status`와 `limit`를 지정합니다.

## Execution Rules
- 실행 전에 필요한 `agent`, `prompt`, `cwd`, `sessionId`, `timeoutMs`, `workflowId`의 정합성을 확인합니다.
- `cwd`에 상대 경로를 전달하는 경우, 호출 측에서 절대 경로로 변환할 수 있음을 확인한 후 사용합니다.
- 현재 base directory 또는 cwd를 확인할 때는 소스 파일이나 기존 세션 추측이 아닌, 대상 agent에 직접 문의합니다.
- Claude와 Codex 양쪽을 비교할 때는 동일 조건의 실행을 `agent=claude`와 `agent=codex`에 대해 각각 실행합니다.
- `sessionId`를 지정하여 계속할 경우, 대상 agent와 대화 목적이 기존 세션과 일치하는지 확인합니다.
- `agent_run_start`의 반환값에 `runId`가 포함되어 있음을 확인한 후 polling을 시작합니다.
- `agent_run_start` 후의 polling에서는 `agent_run_status(runId)`를 반복하고, `completed=true` 또는 실패 상태가 반환될 때까지 확인합니다.
- `agent_run_status(runId)`가 `running=false`이고 `completed=false`를 반환한 경우, 완료 캐시 만료 또는 runId 불일치로 취급하고 `session_get(sessionId)` 또는 로그 확인으로 전환합니다.
- workflow를 사용하는 작업에서는 먼저 `workflow_create`로 workflow를 생성하고, 각 판단 단계에서 `workflow_decision_append`를 추가합니다.
- 파괴적 조작, 외부 전송, 인증 정보 참조, 공유 상태 변경을 위임하는 경우, 상위 정책에서 요구하는 승인 조건을 충족할 때까지 실행하지 않습니다.

## Result Handling
- `agent_run.exitCode`가 `0`이고 `agent_run.status`가 `completed`일 때만 동기 CLI 실행 성공으로 취급합니다.
- `agent_run_start.status`가 `running`이고 `background=true`와 `runId`가 반환되었을 때만 백그라운드 시작 성공으로 취급합니다.
- `agent_run_status(runId).completed=true`일 때는 `result.exitCode`와 `result.status`를 확인하여 최종 성공 또는 실패를 판정합니다.
- `agent_run_status(runId).result`는 완료된 run의 최종 결과로 취급합니다.
- `agent_run_status()`의 `recentCompleted`는 최근 완료 run의 요약으로 취급하고, stdout/stderr 본문을 포함하지 않는 것으로 취급합니다.
- `stdout`은 agent의 응답 본문으로 취급합니다.
- `stderr`는 실행 로그, 경고, 오류의 증거로 확인합니다.
- `sessionId`는 계속 실행, 감사, 이력 확인에 사용할 수 있는 식별자로 저장하거나 보고합니다.
- `runId`는 단일 실행의 상태 확인, 완료 확인, 장애 조사에 사용할 수 있는 식별자로 저장하거나 보고합니다.
- workflow decision의 `decision`, `findings`, `evidenceSummary`는 감사용 구조화 판단으로 취급합니다.

## Reporting
- `agent_run` 실행 후에는 `agent`, `runId`, `sessionId`, `exitCode`, `status`를 보고합니다.
- `agent_run_start` 실행 후에는 `agent`, `runId`, `sessionId`, `status`, `background`를 보고합니다.
- `agent_run_status(runId)` 실행 후에는 `running`, `completed`, `runId`, 최종 `result.status`, 최종 `result.exitCode`를 보고합니다.
- `agent_run_status()` 실행 후에는 `count`, `runs` 건수, `recentCompletedCount`를 보고합니다.
- workflow 조작 후에는 `workflowId`, 추가한 `stage`, `role`, `decision`, 실패 사유를 보고합니다.
- `cwd` 또는 base directory를 확인한 경우, 문의에 사용한 실행의 `stdout`을 근거로 보고합니다.
- 실패 시에는 `stderr`, MCP 응답의 실패 필드, 또는 `agent_run_status`의 상태를 근거로 보고합니다.
- 세션 삭제를 실행한 경우, 대상 `sessionId`, 삭제 결과, 재실행 필요 여부를 보고합니다.

# Must NOT

- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `-p`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `--print`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `--allowedTools` 또는 `--allowed-tools`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `--dangerously-skip-permissions`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `-s` 또는 `--sandbox`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `-a` 또는 `--ask-for-approval`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `--add-dir`를 지정해서는 안 됩니다.
- `agent_run.extraArgs` 또는 `agent_run_start.extraArgs`에 `--dangerously-bypass-approvals-and-sandbox`를 지정해서는 안 됩니다.
- `messages.role`에 `system`을 지정해서는 안 됩니다.
- 기존 세션의 과거 응답만을 근거로 현재의 cwd를 단정해서는 안 됩니다.
- `exitCode` 또는 `status`를 확인하지 않고 동기 실행의 성공을 보고해서는 안 됩니다.
- `agent_run_start`의 시작 성공만을 근거로 최종 실행 성공을 보고해서는 안 됩니다.
- `agent_run_status`의 `recentCompleted`를 stdout/stderr 본문의 대안으로 취급해서는 안 됩니다.
- workflow metadata, workflow decision, prompt summary에 raw prompt, 인증 정보, 토큰, API key, 개인정보를 포함해서는 안 됩니다.
- 사용자가 명시하지 않은 세션을 삭제해서는 안 됩니다.

# Flow

1. `orchestrator_health`로 MCP 서버의 연결 상태를 확인합니다.
2. 도구 사양 또는 파라미터 판단이 불명확한 경우, `orchestrator_usage`를 확인합니다.
3. 판단 로그가 필요한 작업에서는 `workflow_create`로 workflow를 생성합니다.
4. 실행 대상 agent, prompt, session 사용 여부, cwd, timeout, workflow metadata를 결정합니다.
5. 짧은 동기 실행에서는 `agent_run`을 실행합니다.
6. 배치, 장시간, 또는 실행 시간 불명확한 실행에서는 `agent_run_start`를 실행합니다.
7. `agent_run_start`를 사용한 경우, 반환된 `runId`로 `agent_run_status(runId)`를 polling합니다.
8. `completed=true`의 `result` 또는 동기 실행의 반환값에서 `status`, `exitCode`, `stdout`, `stderr`, `sessionId`를 확인합니다.
9. workflow를 사용하는 작업에서는 각 단계의 판단을 `workflow_decision_append`로 추가합니다.
10. 계속 대화가 필요한 경우, 이전 응답의 `sessionId`를 다음 실행에 전달합니다.
11. 이력 감사가 필요한 경우, `session_get` 또는 `workflow_get`으로 대상 이력을 확인합니다.
12. 실행 결과와 근거를 `Reporting`의 규칙에 따라 보고합니다.

# Examples

## Single Short Run Without Session
```json
{
  "agent": "claude",
  "prompt": "Python으로 소수 판별 함수를 작성해주세요.",
  "useSession": false,
  "timeoutMs": 120000
}
```

## Long Or Unknown Duration Run
```json
{
  "agent": "claude",
  "prompt": "장시간 조사를 실행하고 결과를 요약해주세요.",
  "useSession": false,
  "cwd": "/Users/soonyub.hwang/Works/github/lsm/lsm-ai",
  "timeoutMs": 300000
}
```

## Poll Background Run
```json
{
  "runId": "<agent_run_start에서 반환된 runId>"
}
```

## Continue Existing Session
```json
{
  "agent": "claude",
  "prompt": "이전 구현에 에러 핸들링을 추가해주세요.",
  "sessionId": "<previous-session-id>",
  "useSession": true
}
```

## Confirm Current cwd
```json
{
  "agent": "codex",
  "prompt": "현재 작업 디렉터리를 한 줄만 출력해주세요.",
  "useSession": false,
  "timeoutMs": 120000
}
```

## Workflow Decision Logging
```json
{
  "title": "mcp-orchestration-ai skill update",
  "objective": "agent_run_start와 workflow logging의 이용 규칙을 반영한다",
  "metadata": {
    "target": "tools/mcp-orchestration-ai/SKILL.md"
  }
}
```

## Run With Workflow Metadata
```json
{
  "agent": "codex",
  "prompt": "대상 변경의 리뷰를 해주세요.",
  "useSession": false,
  "workflow": {
    "id": "<workflowId>",
    "stage": "result-review",
    "role": "reviewer",
    "promptSummary": "SKILL.md의 result review",
    "expectedDecision": "approved or rejected"
  }
}
```

# Definition of Done

## Verification
- frontmatter에는 `name`과 `description`만 존재합니다.
- frontmatter의 `description`은 한국어 한 문장입니다.
- `# Must`, `# Must NOT`, `# Flow`, `# Examples`, `# Definition of Done`이 존재합니다.
- `# Must`의 규칙은 MCP 이용 범위, 입력, 실행, 결과 처리, 보고를 분리하고 있습니다.
- `# Must NOT`의 규칙은 차단 대상 인수, 금지 role, 미검증 단정, 미승인 삭제, 기밀정보 혼입을 금지합니다.
- `agent_run`, `agent_run_start`, `agent_run_status`, `workflow_create`, `workflow_decision_append`, `workflow_get`, `workflow_list`의 사용 판단이 기술되어 있습니다.
- `agent_run_start`와 `agent_run_status`의 polling 예제가 존재합니다.
- workflow metadata와 workflow decision logging의 입력 규칙이 존재합니다.
- `agent_run_status`의 `completed`, `result`, `recentCompleted`의 취급이 기술되어 있습니다.
- `src/server.py`와 `orchestrator_usage`의 도구 명, 주요 파라미터, 반환값과 상충하지 않습니다.
- Markdown은 UTF-8로 읽을 수 있습니다.

## Monitoring
- 장시간 실행에서는 `agent_run_status(runId)`의 `running`, `completed`, `elapsedSec`, `idleSec`, `stdoutLines`, `stderrLines`를 확인합니다.
- 완료 후 `runId`가 active list에서 사라진 경우, 완료 캐시, `session_get(sessionId)`, 또는 workflow decision을 확인합니다.
- workflow를 사용하는 작업에서는 각 단계의 `stage`, `role`, `decision`, `evidenceSummary`가 추가되어 있는지 확인합니다.
