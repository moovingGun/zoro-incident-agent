"""Zoro-1 판정 서버. 모델을 한 번만 불러 두고 에이전트가 HTTP로 묻는다.

    .venv/bin/python agent/zoro_server.py        # http://127.0.0.1:8765
    POST /decide  {"state": ..., "questions": {...}}  ->  {"answers": {...}, "timing_ms": ...}

따로 띄우는 이유: 에이전트를 고쳐 다시 켤 때마다 모델을 다시 불러오지 않아도 되고,
이 프로세스를 끄면 "모델 호출 실패" 상황을 그대로 재현할 수 있다.
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_DIR = os.path.join(ROOT, "models/Zoro-1")
sys.path.insert(0, MODEL_DIR)
from zoro_decide import Decider  # noqa: E402

PORT = 8765
decider = Decider(MODEL_DIR)
decider.decide({"state": "warmup", "questions": {"q": {"type": "noul"}}})


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._send(200, {"ok": True, "device": decider.device})

    def do_POST(self):
        try:
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self._send(200, decider.decide(request))
        except Exception as e:   # noqa: BLE001 -- 잘못된 요청은 호출한 쪽에 그대로 알려 준다
            self._send(400, {"error": f"{type(e).__name__}: {e}"})

    def log_message(self, *args):
        pass


print(f"zoro server ready on http://127.0.0.1:{PORT} ({decider.device})", flush=True)
HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
