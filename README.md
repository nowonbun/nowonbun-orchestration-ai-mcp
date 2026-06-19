# MCP Orchestration AI

Python에서 `claude -p`와 `codex exec`를 호출하고, MCP Tool과 SQLite 세션 관리를 제공하는 경량 오케스트레이션 서버.

## 개요

- **MCP 서버**(streamable-http)로서 Claude Code 및 다른 MCP client에서 tool 호출 가능
- **Web UI**(port 18765)에서 로컬 브라우저로 schedule job 관리
- SQLite를 통한 대화 기록(session) 영속화
- 외부 의존성은 최소화(`mcp` 패키지만 사용)

## 전제 조건

- Python 3.12+
- `claude` CLI가 설치되어 있어야 함(`claude -p` 동작 확인)
- `codex` CLI 사용 시 `codex exec`가 동작해야 함

## 설정

`src/server.py` 상단 Configuration 섹션에서 설정한다.

```python
BASE_DIR = Path("D:/work")  # CLI 실행 시 기본 작업 디렉터리
DB_PATH = Path("D:/work/security/orchestrator.sqlite")  # SQLite DB 경로
DEFAULT_TIMEOUT_MS = 3000000                   # CLI 실행 타임아웃(ms)
DEFAULT_IDLE_TIMEOUT_SEC = 120                 # 출력이 없을 때 idle timeout(초)
DEFAULT_ALIVE_LOG_INTERVAL_SEC = 30            # 진행 중 alive 로그 간격(초)
DEFAULT_MCP_PORT = 18282                       # MCP 포트
DEFAULT_WEB_PORT = 18765                       # Web UI 포트
```

- `BASE_DIR`: `agent_run`에서 `cwd`를 지정하지 않았을 때 CLI가 실행될 디렉터리. 이 디렉터리의 `.mcp.json`, `CLAUDE.md`, skills가 CLI에 적용된다.
- `DB_PATH`: SQLite 데이터베이스 파일 경로. DB 저장 위치는 Configuration 섹션에서 직접 수정한다.
- `DEFAULT_TIMEOUT_MS`: CLI 실행 hard timeout 기본값.
- `DEFAULT_IDLE_TIMEOUT_SEC`: stdout/stderr 출력이 없는 상태가 유지될 때 적용되는 idle timeout 기본값.
- `DEFAULT_ALIVE_LOG_INTERVAL_SEC`: 장시간 실행 중 `cli.alive` 로그를 남기는 기본 간격.
- `D:/work/security`: `filePaths` 주입 경로에서 차단되는 보안 루트다.

## 실행 방법

```bash
cd D:/work/nowonbun-orchestration-ai-mcp
python src/server.py
```

실행하면 아래 두 서버가 동시에 시작된다:

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
      "type": "http",
      "url": "http://127.0.0.1:18282/mcp/"
    }
  }
}

claude mcp add --transport http orchestration-ai http://127.0.0.1:18282/mcp/
```

사전에 `python src/server.py`로 서버를 실행해 두어야 한다.

## Codex에 등록

Codex 설정 파일 `~/.codex/config.toml`에 추가한다.

```toml
[mcp_servers.mcp-orchestration]
type = "url"
url = "http://127.0.0.1:18282/mcp/"
```

사전에 `python src/server.py`로 서버를 실행해 두어야 한다.

## MCP Tool 목록

| Tool | 설명 |
|---|---|
| `orchestrator_usage` | 사용 가이드 반환(AI용) |
| `orchestrator_health` | 서버 상태와 DB 경로 반환 |
| `session_create` | 세션 생성(초기 메시지 설정 가능) |
| `session_get` | 세션과 메시지 기록 조회 |
| `session_list` | 최근 세션 목록 조회 |
| `session_append` | 기존 세션에 메시지 수동 추가 |
| `session_delete` | 세션과 관련 메시지 삭제 |
| `agent_run` | Claude/Codex CLI 실행 후 결과를 세션에 저장 |
| `agent_run_status` | 현재 실행 중인 `agent_run`의 프로세스/I/O 상태 조회 |

## CLI 실행 사양

| Agent | 명령 형식 |
|---|---|
| Claude | `claude -p <prompt> [--allowedTools <pattern>]` |
| Codex | `codex exec --skip-git-repo-check <prompt>` |

- 역할은 `user`와 `assistant`만 사용 가능(`system`은 미지원)
- `cwd` 미지정 시 `BASE_DIR`에서 실행된다

### `agent_run` 입력 제약

- `filePaths`의 상대경로는 `cwd`가 있으면 그 경로를 기준으로, 없으면 `BASE_DIR`을 기준으로 해석된다.
- `filePaths`로 `D:/work/security` 하위 경로는 읽을 수 없다.
- `extraArgs`에는 `--allowedTools`, `--allowed-tools`, `--dangerously-skip-permissions`, `--sandbox`, `--ask-for-approval`, `--add-dir`, `--dangerously-bypass-approvals-and-sandbox`를 넣을 수 없다.
- `skipPermissions`는 `agent="claude"`일 때만 내부적으로 `--dangerously-skip-permissions`를 추가하며, 다른 agent에서는 전달해도 효과가 없다.

### timeout 동작

- `timeoutMs`는 전체 실행 시간에 대한 hard timeout으로 동작한다.
- stdout/stderr 출력이 `ORCH_IDLE_TIMEOUT_SEC` 동안 없으면 idle timeout으로 중단된다.
- `ORCH_IDLE_TIMEOUT_SEC`를 지정하지 않으면 `DEFAULT_IDLE_TIMEOUT_SEC`(기본 120초)를 사용한다.
- `ORCH_ALIVE_LOG_INTERVAL_SEC`를 지정하지 않으면 `DEFAULT_ALIVE_LOG_INTERVAL_SEC`(기본 30초) 간격으로 `cli.alive` 로그를 남긴다.
- timeout 발생 시 서버는 먼저 terminate를 시도하고, 종료되지 않으면 kill로 정리한다.

### `agent_run_status` 동작

- `agent_run_status()`는 현재 실행 중인 run만 보여준다. 완료된 run은 목록에서 제거된다.
- `runId`를 생략하면 현재 활성 run 전체를 반환한다. 다른 클라이언트나 다른 PC에서 진행 상태를 확인할 때 기본적으로 이 방식을 사용한다.
- `runId`를 지정하면 해당 run이 아직 실행 중인지와 stdout/stderr 활동 기준 idle 시간, 누적 출력 줄 수를 조회할 수 있다.
- 이 상태 조회는 프로세스/표준출력/표준에러 활동만 관찰한다. 모델 내부 추론 단계 자체를 직접 보여주지는 않는다.

## 파일 구성

```text
mcp-orchestration-ai/
├── README.md
├── PLANNING.md          # 상세 설계 문서
├── SKILL.md             # AI용 사용 스킬 정의
├── .gitignore
└── src/
    └── server.py            # 구현 본체(단일 파일 구성)
```

- SQLite DB 기본 위치는 저장소 내부가 아니라 `D:/work/security/orchestrator.sqlite`다.

## 테스트

```bash
cd D:/work/nowonbun-orchestration-ai-mcp
python3 -m py_compile src/server.py
```

## 보안

- Web UI는 로컬 개발용이며 인증 기능이 없다(`127.0.0.1` bind)
- prompt / message 저장 시 `api_key`, `token`, `secret`, `password`, `Authorization: Bearer` 패턴은 `<redacted>`로 치환한다
- 기밀 정보의 완전한 탐지는 보장하지 않는다. secret을 prompt에 포함하지 않는 운영을 전제로 한다
- `claude` / `codex` CLI 인증·MCP 설정·PAT 관리는 서버 외부에서 수행한다

## 상세 설계

아키텍처, SQLite schema, Scheduler 사양, CLI 실행 사양의 상세 내용은 [PLANNING.md](./PLANNING.md)를 참조한다.
AI가 도구를 사용할 때의 절차·제약은 [SKILL.md](./SKILL.md)를 참조한다.
