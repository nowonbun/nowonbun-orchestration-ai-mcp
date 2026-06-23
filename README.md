# MCP Orchestration AI

`claude -p`와 `codex exec`를 Python에서 호출하고, MCP Tool과 SQLite 세션 관리를 제공하는 경량 오케스트레이션 서버.

## 개요

- **MCP 서버**(streamable-http)로서 Claude Code나 다른 MCP client에서 tool 호출 가능
- **Web UI**(port 18765)로 로컬 브라우저에서 schedule job을 관리
- SQLite를 통한 대화 이력(session) 영속화
- 외부 의존성은 최소한(`mcp` 패키지만)

## 전제 조건

- Python 3.12+
- `claude` CLI가 설치 완료(`claude -p`가 동작할 것)
- `codex` CLI(Codex 연동을 사용하는 경우, `codex exec`가 동작할 것)

## 아키텍처

```text
MCP Client (Claude Code / Codex / etc.)
    │
    └─ streamable-http (port 18282)
         └─ FastMCP tools

Browser
    │
    └─ HTTP localhost (port 18765)
         └─ ThreadingHTTPServer (Web UI)

src/server.py
    ├─ Configuration       # BASE_DIR, DEFAULT_TIMEOUT_MS
    ├─ SessionStore        # SQLite schema / CRUD / agent 실행
    ├─ run_agent_cli       # claude -p / codex exec subprocess
    ├─ compile_claude_parts / compile_codex_prompt  # 메시지→프롬프트 변환
    ├─ ScheduleRunner      # 프로세스 내부 cron-like runner
    ├─ UiHandler           # 로컬 Web UI
    └─ main                # MCP streamable-http 시작
```

## 설정

`src/server.py` 상단의 Configuration 섹션에서 설정한다.

```python
BASE_DIR = Path("/Users/soonyub.hwang/cron")  # CLI 실행 시 기본 작업 디렉토리
DEFAULT_TIMEOUT_MS = 300000                    # CLI 실행 타임아웃(ms)
```

- `BASE_DIR`: `agent_run`에서 `cwd` 미지정 시 CLI가 실행되는 디렉토리. 이 디렉토리의 `.mcp.json`, `CLAUDE.md`, skills가 CLI에 적용된다.
- `DEFAULT_TIMEOUT_MS`: CLI 실행의 기본 타임아웃.

### 환경변수

| 환경변수 | 기본값 | 설명 |
|---|---|---|
| `ORCH_HOST` | `127.0.0.1` | MCP 서버 bind host |
| `ORCH_PORT` | `18282` | MCP 서버 port |
| `ORCH_DB_PATH` | `<root>/data/orchestrator.sqlite` | SQLite DB 경로 |
| `ORCH_SCHEDULER_INTERVAL_SECONDS` | `30` | due job polling 간격(초) |
| `ORCH_WEB_ENABLED` | `true` | Web UI 시작 여부 |
| `ORCH_WEB_HOST` | `127.0.0.1` | Web UI bind host |
| `ORCH_WEB_PORT` | `18765` | Web UI port |
| `ORCH_DEBUG` | `true` | 디버그 모드 |
| `ORCH_LOG_LEVEL` | `DEBUG` | console log level |

## 실행 방법

```bash
cd lsm-ai/tools/mcp-orchestration-ai
python src/server.py
```

실행하면 다음 2개의 서버가 동시에 시작된다:

| 서버 | 기본 주소 | 설명 |
|---|---|---|
| MCP (streamable-http) | `http://127.0.0.1:18282/mcp/` | MCP client용 엔드포인트 |
| Web UI | `http://127.0.0.1:18765` | schedule job 관리 화면 |

## Claude Code에 등록

Claude Code의 MCP 설정(`~/.claude/settings.json` 또는 프로젝트 `.mcp.json`)에 추가한다.

```json
{
  "mcpServers": {
    "orchestration-ai": {
      "type": "url",
      "url": "http://127.0.0.1:18282/mcp/"
    }
  }
}
```

또는 CLI로 등록:

```bash
claude mcp add --transport http orchestration-ai http://127.0.0.1:18282/mcp/
```

사전에 `python src/server.py`로 서버를 실행해 둘 것.

## Codex에 등록

Codex의 설정 파일 `~/.codex/config.toml`에 추가한다.

```toml
[mcp_servers.mcp-orchestration]
type = "url"
url = "http://127.0.0.1:18282/mcp/"
```

사전에 `python src/server.py`로 서버를 실행해 둘 것.
mcp-orchestration-ai는 Codex의 MCP tool 승인, sandbox, add-dir 설정을 생성하지 않는다. Codex 측의 승인 정책은 Codex의 설정 파일 또는 Codex CLI의 기본 동작으로 관리한다.
Codex에서 MCP tool 호출을 사전 승인하려면, Codex의 `config.toml`에 대상 server/tool별 설정을 추가한다.

```toml
# 대상 tool을 승인하는 경우
[mcp_servers.<server>.tools.<tool>]
approval_mode = "approve"

# 또는, Codex 측의 확인을 비활성화하는 경우
approval_policy = "never"
```

`<server>`와 `<tool>`은 Codex 측의 MCP server 이름과 tool 이름으로 대체한다.

## MCP Tool 목록

| Tool | 설명 |
|---|---|
| `orchestrator_usage` | 사용 가이드를 반환(AI용) |
| `orchestrator_health` | 서버 상태와 DB 경로를 반환 |
| `session_create` | 세션 생성(초기 메시지 설정 가능) |
| `session_get` | 세션과 메시지 이력을 조회 |
| `session_list` | 최근 세션 목록을 조회 |
| `session_append` | 기존 세션에 메시지를 수동 추가 |
| `session_delete` | 세션과 관련 메시지를 삭제 |
| `workflow_create` | workflow 판단 로그를 시작 |
| `workflow_decision_append` | workflow에 판단 로그를 추가 |
| `workflow_get` | workflow와 판단 로그를 조회 |
| `workflow_list` | workflow 목록을 조회 |
| `agent_run` | Claude/Codex CLI를 실행하고 결과를 세션에 저장 |
| `agent_run_start` | Claude/Codex CLI를 백그라운드로 시작하고 runId를 반환 |
| `agent_run_status` | 실행 중 또는 최근 완료된 run의 상태를 조회 |

스케줄 관련 MCP tool은 제공하지 않는다(Web UI에서만 조작 가능).

## CLI 실행 사양

| Agent | 커맨드 형식 |
|---|---|
| Claude | `claude -p <prompt> [--allowedTools <pattern>] --dangerously-skip-permissions` |
| Codex | `codex exec --skip-git-repo-check <prompt>` |

- role은 `user`와 `assistant`만 지원(`system`은 미지원)
- `cwd` 미지정 시 `BASE_DIR`에서 실행됨
- `skipPermissions`, `codexMcpApprovedTools`, `codexMcpApprovedWriteTools`, `approveCodexMcpWrites`는 호환용으로 수신하지만, agent 실행 로직에는 전달되지 않음.
- `extraArgs`는 Claude 실행에만 전달됨. Codex 실행에서는 사용자 지정 `extraArgs`가 전달되지 않으며, `--skip-git-repo-check`만 항상 부여됨.

### 차단 대상 extraArgs

다음은 `extraArgs`에 포함하면 ValueError가 발생한다:

- `-p`, `--print`
- `--allowedTools`, `--allowed-tools`
- `--dangerously-skip-permissions`

### 메시지와 role

- 허용 role: `user`, `assistant`, `tool`
- `system` role은 미지원
- 세션 사용 시 과거 user/assistant 메시지가 자동으로 프롬프트에 포함됨

#### Claude용 프롬프트 컴파일

과거 메시지를 `[이전 질문]` / `[이전 답변]` 라벨로 선두에 배치하고, 마지막에 현재 user 메시지를 배치.

#### Codex용 프롬프트 컴파일

과거 메시지를 `이전 요청:` / `이전 응답:` 라벨로 선두에 배치하고, 마지막에 현재 user 메시지를 배치.

### Codex sandbox와 쓰기 대상

Codex CLI는 mcp-orchestration-ai 서버 프로세스와 별도로 Codex 측의 sandbox에서 실행된다. Codex가 `workspace-write` sandbox로 실행된 경우, 쓰기 가능한 범위는 보통 `cwd`(workdir), `/tmp`, `$TMPDIR` 등의 허용된 writable root로 제한된다.

따라서, `cwd` 외부에 있는 `/Users/soonyub.hwang/desk/DailyWork` 등으로 Codex에서 직접 파일을 쓰면, `PermissionError`로 실패할 수 있다. 외부 디렉토리에 파일을 쓰는 job에서는, `agent_run` / `agent_run_start` / schedule job의 `cwd`를 대상 디렉토리로 지정하거나, 허용된 writable root에 생성한 결과물을 별도 절차로 배치한다.

mcp-orchestration-ai는 Codex의 `--sandbox`, `--add-dir`, approval 관련 CLI 옵션을 생성하지 않는다. 이러한 제어가 필요한 경우는, Codex 측의 설정 또는 별도의 실행 경로에서 처리한다.

## SQLite Schema

### `sessions`

```sql
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  title TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### `messages`

```sql
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
```

### `scheduled_jobs`

```sql
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
```

### `scheduled_runs`

```sql
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
```

### Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session_id_sort_order ON messages(session_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(enabled, running, next_run_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_runs_job_started ON scheduled_runs(job_id, started_at DESC);
```

## 스케줄

Web UI에서 스케줄 job을 등록 가능. cron expression은 5-field 형식(분 시 일 월 요일).

### Scheduler 동작

Scheduler는 Python 프로세스 내에서 동작한다. 외부 cron, Docker, systemd timer는 대상 외.

1. `scheduled_jobs.enabled = 1`이면서 `next_run_at <= now`인 job을 조회
2. job의 `agent`에 따라 `claude -p` 또는 `codex exec`를 실행
3. 실행 결과를 `scheduled_runs`에 저장
4. `last_run_at`과 `next_run_at`을 갱신

### cron 지원 범위

5-field cron: `* */n 숫자 a-b a,b`

| field | range |
|---|---|
| minute | 0-59 |
| hour | 0-23 |
| day | 1-31 |
| month | 1-12 |
| weekday | 0-6 |

### 수동 실행 전용

cron expression에 `- - - - - -`를 지정하면, 자동 실행하지 않고 수동 실행(Run Now)만 가능한 job으로 등록된다.

- `_is_manual_only_cron()`에서 `_parse_cron()` 도달 전에 가드됨
- `_next_cron_run()`은 `None`을 반환하고, `next_run_at`은 NULL로 저장됨
- `list_due_schedule_ids`의 `WHERE next_run_at <= ?` 조건에 NULL은 매칭되지 않으므로 자동 실행되지 않음
- Web UI에서는 cron 란에 "수동 실행 전용"으로 표시됨

## Web UI

Web UI는 로컬 운영용 최소 화면. 스케줄 job의 생성, 목록, 활성화/정지, 수동 실행, 실행 이력 표시를 제공한다.

- 인증 없음(`127.0.0.1` bind)
- 스케줄 관련 MCP tool은 삭제되었지만, Web UI와 내부 ScheduleRunner는 계속 동작
- form submit은 fetch API로 비동기 처리. 성공 시 페이지 리로드, 오류 시 toast 알림(입력 데이터 유지)
- Run Now 등 실행 중에는 loading overlay로 전체 화면을 덮고, 버튼 클릭을 차단

## 파일 구성

```text
mcp-orchestration-ai/
├── README.md
├── PLANNING.md          # 상세 설계 문서 (일본어 원문)
├── SKILL.md             # AI용 사용 스킬 정의
├── .gitignore
├── data/
│   └── orchestrator.sqlite  # SQLite DB(자동 생성, .gitignore 대상)
└── src/
    └── server.py            # 구현 본체(단일 파일 구성)
```

## 테스트

```bash
cd lsm-ai/tools/mcp-orchestration-ai
python3 -m py_compile src/server.py
```

## 보안

- Web UI는 로컬 개발용이며 인증 기능이 없음(`127.0.0.1` bind)
- prompt / message 저장 시 `api_key`, `token`, `secret`, `password`, `Authorization: Bearer` 패턴은 `<redacted>`로 치환
- 기밀 정보의 완전한 검출은 보장하지 않음. secret을 prompt에 포함하지 않는 운영을 전제로 함
- `claude` / `codex` CLI의 인증, MCP 설정, PAT 관리는 서버 외부에서 수행
- Docker 운영은 대상 외. PAT나 MCP 인증 정보를 호스트 측에서 다루기 때문

## 비목표

- Docker container화
- 외부 crontab 등록
- 인증 기능이 있는 multi-user Web UI
- `system` role 지원
- 스케줄 조작의 MCP tool 공개
- 완전한 secret scanner
- production용 감사 로그 기반
