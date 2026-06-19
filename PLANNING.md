# MCP Orchestration AI — 설계 문서

## 개요

`mcp-orchestration-ai`는 Python에서 `claude -p`와 `codex exec`를 호출하고, MCP Tool, SQLite 세션 관리, 로컬 Web UI, 프로세스 내부 scheduler를 제공하는 경량 오케스트레이션 서버다.

단일 파일 구성을 우선한다. Docker 운영은 범위에서 제외한다.

---

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

---

## 파일 구성

```text
mcp-orchestration-ai/
├── README.md
├── PLANNING.md
├── SKILL.md
├── .gitignore
├── data/
│   └── orchestrator.sqlite  （자동 생성）
└── src/
    └── server.py
```

---

## 설정

`src/server.py` 상단에서 직접 설정한다.

```python
BASE_DIR = Path("/Users/soonyub.hwang/desk")
DEFAULT_TIMEOUT_MS = 3000000
```

| 설정 | 설명 |
|---|---|
| `BASE_DIR` | `agent_run`에서 `cwd` 미지정 시 사용할 CLI 작업 디렉터리. 이 디렉터리의 Claude/Codex 프로젝트 설정이 적용된다 |
| `DEFAULT_TIMEOUT_MS` | CLI 실행 기본 타임아웃(ms) |

환경 변수(서버 시작 매개변수):

| 환경 변수 | 기본값 | 설명 |
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

---

## CLI 실행 사양

### Claude

```text
claude -p <prompt> [--allowedTools <pattern>] [extra_args...]
```

`subprocess.Popen(command, cwd=resolved_cwd)`로 실행한다. `resolved_cwd`는 `agent_run.cwd` 지정값 또는 `BASE_DIR`이다.

### Codex

```text
codex exec --skip-git-repo-check <prompt> [extra_args...]
```

### 차단 대상 extraArgs

아래 값이 `extraArgs`에 포함되면 ValueError가 발생한다:

- `-p`, `--print`
- `--allowedTools`, `--allowed-tools`
- `--dangerously-skip-permissions`

---

## SQLite schema

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
  cwd TEXT,
  timeout_ms INTEGER,
  allowed_tools_pattern TEXT,
  extra_args_json TEXT,
  use_session INTEGER NOT NULL DEFAULT 1,
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
  session_id TEXT,
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

### indexes

```sql
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session_id_sort_order ON messages(session_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(enabled, running, next_run_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_runs_job_started ON scheduled_runs(job_id, started_at DESC);
```

---

## MCP Tool 정의

| Tool | 설명 |
|---|---|
| `orchestrator_usage` | 사용 가이드 반환 |
| `orchestrator_health` | 서버 상태 확인 |
| `session_create` | 세션 생성(초기 메시지 설정 가능) |
| `session_get` | 세션과 메시지 기록 조회 |
| `session_list` | 최근 세션 목록 조회 |
| `session_append` | 기존 세션에 메시지 수동 추가 |
| `session_delete` | 세션과 관련 메시지 삭제 |
| `agent_run` | Claude/Codex CLI 실행, 결과를 세션에 저장 |

스케줄 관련 MCP tool은 제공하지 않는다(Web UI에서만 조작 가능).

---

## 메시지와 역할

- 허용 역할: `user`, `assistant`, `tool`
- `system` 역할은 미지원(제거됨)
- 세션 사용 시 이전 user/assistant 메시지가 자동으로 프롬프트에 포함된다

### 프롬프트 컴파일

#### Claude용

이전 메시지를 `[이전 질문]` / `[이전 답변]` 라벨과 함께 앞부분에 배치하고, 마지막에 현재 user 메시지를 배치한다.

#### Codex용

이전 메시지를 `이전 요청:` / `이전 응답:` 라벨과 함께 앞부분에 배치하고, 마지막에 현재 user 메시지를 배치한다.

---

## Web UI

Web UI는 로컬 운영용 최소 화면이다. 스케줄 job 생성·목록·활성화/중지·수동 실행·실행 기록 표시를 제공한다.

- 인증 없음(`127.0.0.1` bind)
- 스케줄 관련 MCP tool은 제거되었지만 Web UI와 내부 ScheduleRunner는 계속 동작한다

---

## Scheduler

Scheduler는 Python 프로세스 내부에서 동작한다. 외부 cron, Docker, systemd timer는 범위에서 제외한다.

### 동작

1. `scheduled_jobs.enabled = 1` 이고 `next_run_at <= now` 인 job을 조회한다
2. job의 `agent`에 따라 `claude -p` 또는 `codex exec`를 실행한다
3. 실행 결과를 `scheduled_runs`에 저장한다
4. `last_run_at`와 `next_run_at`를 갱신한다

### cron 지원 범위

5-field cron: `* */n 숫자 a-b a,b`

| field | range |
|---|---|
| minute | 0-59 |
| hour | 0-23 |
| day | 1-31 |
| month | 1-12 |
| weekday | 0-6 |

---

## 보안

1. Web UI는 로컬 개발·개인 운영용이다. 인증 기능은 없다
2. Docker 운영은 대상이 아니다. PAT 및 MCP 인증 정보를 호스트 측에서 다루기 때문이다
3. prompt나 message 저장 시 `api_key`, `token`, `secret`, `password`, `Authorization: Bearer`는 `<redacted>`로 치환한다
4. 기밀 정보의 완전한 탐지는 보장하지 않는다
5. `claude` / `codex` CLI 인증, MCP 설정, PAT 관리는 서버 외부에서 수행한다

---

## 비목표

- Docker container 화
- 외부 crontab 등록
- 인증이 있는 multi-user Web UI
- `system` 역할 지원
- 스케줄 조작용 MCP tool 공개
- 완전한 secret scanner
- production용 감사 로그 기반
