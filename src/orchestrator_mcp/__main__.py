from __future__ import annotations

import os
from pathlib import Path

from .db import connect_database
from .mcp_server import McpServer
from .session_store import SessionStore
from .sse_server import SseServer


def main() -> None:
    root_dir = Path(__file__).resolve().parents[2]
    db_path = Path(os.getenv("ORCH_DB_PATH", str(root_dir / "data" / "orchestrator.sqlite"))).resolve()
    sse_host = os.getenv("ORCH_SSE_HOST", "127.0.0.1")
    sse_port = int(os.getenv("ORCH_SSE_PORT", "8765"))
    cache_limit = int(os.getenv("ORCH_CACHE_LIMIT", "100"))
    default_timeout_ms = int(os.getenv("ORCH_DEFAULT_TIMEOUT_MS", "120000"))

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(str(db_path))
    sse_server = SseServer(sse_host, sse_port)
    store = SessionStore(
        connection=connection,
        db_path=str(db_path),
        cache_limit=cache_limit,
        default_timeout_ms=default_timeout_ms,
        sse_server=sse_server,
    )
    mcp_server = McpServer(
        name="nowonbun-orchestration-ai-mcp",
        version="0.1.0",
        store=store,
        sse_server=sse_server,
    )

    sse_server.start()
    try:
        mcp_server.serve_forever()
    finally:
        sse_server.stop()
        connection.close()


if __name__ == "__main__":
    main()
