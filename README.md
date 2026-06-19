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
BASE_DIR = Path("/Users/soonyub.hwang/desk")  # CLI 실행 시 기본 작업 디렉터리
DB_PATH = Path("/Users/soonyub.hwang/desk/data/orchestrator.sqlite")  # SQLite DB 경로
DEFAULT_TIMEOUT_MS = 3000000                   # CLI 실행 타임아웃(ms)
```

- `BASE_DIR`: `agent_run`에서 `cwd`를 지정하지 않았을 때 CLI가 실행될 디렉터리. 이 디렉터리의 `.mcp.json`, `CLAUDE.md`, skills가 CLI에 적용된다.
- `DB_PATH`: SQLite 데이터베이스 파일 경로. DB 저장 위치는 Configuration 섹션에서 직접 수정한다.
- `DEFAULT_TIMEOUT_MS`: CLI 실행 기본 타임아웃.

## 실행 방법

```bash
cd lsm-ai/tools/mcp-orchestration-ai
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
      "type": "url",
      "url": "http://127.0.0.1:18282/mcp/"
    }
  }
}
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

## CLI 실행 사양

| Agent | 명령 형식 |
|---|---|
| Claude | `claude -p <prompt> [--allowedTools <pattern>]` |
| Codex | `codex exec --skip-git-repo-check <prompt>` |

- 역할은 `user`와 `assistant`만 사용 가능(`system`은 미지원)
- `cwd` 미지정 시 `BASE_DIR`에서 실행된다

## 파일 구성

```text
mcp-orchestration-ai/
├── README.md
├── PLANNING.md          # 상세 설계 문서
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

- Web UI는 로컬 개발용이며 인증 기능이 없다(`127.0.0.1` bind)
- prompt / message 저장 시 `api_key`, `token`, `secret`, `password`, `Authorization: Bearer` 패턴은 `<redacted>`로 치환한다
- 기밀 정보의 완전한 탐지는 보장하지 않는다. secret을 prompt에 포함하지 않는 운영을 전제로 한다
- `claude` / `codex` CLI 인증·MCP 설정·PAT 관리는 서버 외부에서 수행한다

## 상세 설계

아키텍처, SQLite schema, Scheduler 사양, CLI 실행 사양의 상세 내용은 [PLANNING.md](./PLANNING.md)를 참조한다.
AI가 도구를 사용할 때의 절차·제약은 [SKILL.md](./SKILL.md)를 참조한다.
