"""
NetScope Controlled Test Endpoint — M7 / FR_12

A controllable HTTP server designed for fault-injection testing:
- Normal mode: Responds in ~10-30ms with HTTP 200
- Injected Latency mode: Adds configurable delay (e.g. 500ms, 1000ms)
- Injected Error mode: Returns HTTP 500 / 503 errors
- Intermittent failure mode: Randomly fails X% of requests

API Controls:
  GET  /health           -> Measured target endpoint
  POST /inject/delay     -> Body: {"delay_ms": 500}
  POST /inject/error     -> Body: {"status_code": 500, "rate": 1.0}
  POST /reset            -> Restores normal healthy operation
  GET  /status           -> Returns current chaos injection settings
"""

from __future__ import annotations

import json
import logging
import random
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

logger = logging.getLogger("netscope.controlled_server")

SERVER_STATE = {
    "delay_ms": 0.0,
    "error_code": 200,
    "error_rate": 0.0,  # 0.0 = never fail, 1.0 = always fail
}


class ControlledHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/status":
            self._handle_status()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        post_body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            payload = json.loads(post_body.decode("utf-8")) if post_body else {}
        except Exception:
            payload = {}

        if self.path == "/inject/delay":
            SERVER_STATE["delay_ms"] = float(payload.get("delay_ms", 500.0))
            self._json_response(200, {"message": f"Injected delay set to {SERVER_STATE['delay_ms']} ms", "state": SERVER_STATE})
        elif self.path == "/inject/error":
            SERVER_STATE["error_code"] = int(payload.get("status_code", 500))
            SERVER_STATE["error_rate"] = float(payload.get("rate", 1.0))
            self._json_response(200, {"message": "Injected error state updated", "state": SERVER_STATE})
        elif self.path == "/reset":
            SERVER_STATE["delay_ms"] = 0.0
            SERVER_STATE["error_code"] = 200
            SERVER_STATE["error_rate"] = 0.0
            self._json_response(200, {"message": "Endpoint restored to healthy operation", "state": SERVER_STATE})
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_health(self):
        # 1. Apply artificial delay if injected
        delay_sec = SERVER_STATE["delay_ms"] / 1000.0
        if delay_sec > 0:
            time.sleep(delay_sec)

        # 2. Check if error should be injected
        status = 200
        if SERVER_STATE["error_rate"] > 0:
            if random.random() < SERVER_STATE["error_rate"]:
                status = SERVER_STATE["error_code"]

        body = {
            "status": "healthy" if status == 200 else "degraded",
            "injected_delay_ms": SERVER_STATE["delay_ms"],
            "server_time": time.time(),
        }
        self._json_response(status, body)

    def _handle_status(self):
        self._json_response(200, SERVER_STATE)

    def _json_response(self, status: int, data: dict):
        response_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def log_message(self, format, *args):
        # Quiet standard HTTP access logging to prevent console spam
        pass


def run_controlled_server(port: int = 8080) -> None:
    server = HTTPServer(("0.0.0.0", port), ControlledHandler)
    logger.info("Controlled Test Server running on http://0.0.0.0:%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    run_controlled_server(8080)
