from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


@dataclass
class SseClient:
    writer: Any
    session_id: str | None


class SseServer:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._clients: dict[str, SseClient] = {}
        self._lock = threading.Lock()
        self._sequence = 0

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def start(self) -> None:
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    payload = json.dumps({"ok": True, "host": parent.host, "port": parent.port}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                if parsed.path != "/events":
                    self.send_error(404, "not found")
                    return

                session_id = parse_qs(parsed.query).get("sessionId", [None])[0]
                with parent._lock:
                    parent._sequence += 1
                    client_id = f"{int(time.time() * 1000)}-{parent._sequence}"
                    parent._clients[client_id] = SseClient(self.wfile, session_id)

                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-transform")
                self.send_header("Connection", "keep-alive")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(self._format_event("server.ready", {"clientId": client_id, "sessionId": session_id}))
                self.wfile.flush()

                try:
                    while True:
                        time.sleep(15)
                        self.wfile.write(self._format_event("heartbeat", {"ts": parent._now_iso()}))
                        self.wfile.flush()
                except Exception:
                    pass
                finally:
                    with parent._lock:
                        parent._clients.pop(client_id, None)

            def _format_event(self, event: str, payload: dict) -> bytes:
                return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def broadcast(self, event: str, payload: dict | None = None, *, session_id: str | None = None) -> None:
        payload = {"ts": self._now_iso(), **(payload or {})}
        data = f"id: {int(time.time() * 1000)}\nevent: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")

        with self._lock:
            client_snapshot = [
                (client_id, client)
                for client_id, client in self._clients.items()
                if not session_id or not client.session_id or session_id == client.session_id
            ]

        stale_ids: list[str] = []
        for client_id, client in client_snapshot:
            try:
                client.writer.write(data)
                client.writer.flush()
            except Exception:
                stale_ids.append(client_id)

        if stale_ids:
            with self._lock:
                for stale_id in stale_ids:
                    self._clients.pop(stale_id, None)

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
