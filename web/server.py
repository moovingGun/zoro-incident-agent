"""웹 화면 서버: 정적 파일 + 실시간 상태 + 담당자 인수.

    .venv/bin/python web/server.py                       # http://localhost:8090
    OPERATOR_TOKEN=비밀값 .venv/bin/python web/server.py   # 인수 버튼은 ?operator=비밀값 으로 연 화면에서만
    WEB_HOST=0.0.0.0 .venv/bin/python web/server.py      # 다른 컴퓨터에서도 접속 (기본은 이 컴퓨터에서만)

    GET  /api/live   에이전트 기록과 정상 클라이언트 기록을 이어 읽어 지금 상태를 돌려준다
    POST /api/ack    {"incident": "...", "token": "..."}  이관된 사건을 담당자가 인수했다고 기록한다

기록 재생(data/replay.json)은 정적 파일이라 이 서버 없이도 열린다.
"""
import json
import os
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from incident_view import PATH_NAMES, build, decided_clock, verdict_row  # noqa: E402

EVENTS = os.path.join(ROOT, "logs/agent/events.jsonl")
REQUESTS = os.path.join(ROOT, "logs/client/requests.jsonl")
PORT = 8090
HOST = os.environ.get("WEB_HOST", "127.0.0.1")   # 밖에서 접속하게 하려면 0.0.0.0. 그때는 OPERATOR_TOKEN도 같이 건다
IDLE_SPAN = 30        # 사건이 없을 때 보여 줄 최근 구간(초)
KEEP_AFTER_END = 120  # 복구로 끝난 사건을 이만큼 더 보여 준다(초)
KEEP_AFTER_RESUME = 15  # 이관했던 사건은 감시가 재개되면 이만큼만 더 보여 주고 지금 상태로 넘어간다(초)
RETAIN = 900          # 메모리에 두는 기록의 길이(초). 마지막 사건 주변은 이보다 오래돼도 둔다
OPERATOR_TOKEN = os.environ.get("OPERATOR_TOKEN")   # 설정하면 이 값을 아는 화면에서만 인수할 수 있다


class Log:
    """덧붙는 JSONL 파일을 이어 읽는다. 파일 끝부분만 다시 읽지 않으므로 사건 시작 줄이 밀려나 사라지지 않는다."""

    def __init__(self, path):
        self.path, self.pos, self.buf = path, 0, b""

    def read(self):
        try:
            size = os.path.getsize(self.path)
        except OSError:
            return []
        if size < self.pos:           # 파일이 비워졌거나 바뀌었다: 처음부터
            self.pos, self.buf = 0, b""
        with open(self.path, "rb") as f:
            f.seek(self.pos)
            data = f.read()
            self.pos = f.tell()
        *lines, self.buf = (self.buf + data).split(b"\n")
        rows = []
        for x in lines:
            try:
                rows.append(json.loads(x))
            except ValueError:
                pass
        return rows


lock = threading.Lock()            # 기록 읽기와 인수 기록을 한 번에 하나씩
event_log, request_log = Log(EVENTS), Log(REQUESTS)
events, requests, baselines = [], {}, []
escalations = {}                   # 이관된 사건 요약. 오래된 기록을 버려도 인수하지 않은 사건은 남긴다


def refresh(now):
    """새로 덧붙은 줄을 반영하고 오래된 것을 버린다. lock을 잡고 부른다."""
    global events, requests
    for e in event_log.read():
        events.append(e)
        if e["type"] == "baseline":
            baselines.append(e)
        elif e["type"] == "incident":
            escalations[e["incident"]] = {"id": e["incident"], "fault": e["fault"], "escalated_t": None}
        elif e.get("incident") in escalations:
            x = escalations[e["incident"]]
            if e["type"] == "escalated":
                x.update(escalated_t=e["t"], kind=e.get("kind") or "not_recovered", reason=e.get("reason", ""),
                         acknowledged=False, resumed=False)
            elif e["type"] == "acknowledged":
                x["acknowledged"] = True
            elif e["type"] in ("monitoring_resumed", "closed"):
                x["resumed"] = True
    for r in request_log.read():
        if r["ev"] == "start":
            requests[r["id"]] = {"path": r["path"], "start": r["t"], "t": None, "result": None, "ms": None}
        else:
            requests[r["id"]] = {"path": r["path"], "start": r["start"], "t": r["t"], "result": r["result"], "ms": r["ms"]}
    opened = [e["t"] for e in events if e["type"] == "incident"]
    keep = min(now - RETAIN, opened[-1] - 60) if opened else now - RETAIN
    if events and events[0]["t"] < keep - 60:
        events = [e for e in events if e["t"] >= keep]
        requests = {k: r for k, r in requests.items() if r["start"] >= keep}


def with_baselines():
    """사건 당시의 평소 기준을 찾을 수 있게, 버린 기록에 있던 기준 줄도 함께 넘긴다."""
    return [b for b in baselines if not events or b["t"] < events[0]["t"]] + events


def idle_view(now):
    lo = now - IDLE_SPAN
    when = decided_clock(events)
    return {"id": None, "outcome": "idle", "origin_label": "현재", "axis_label": "초 (0 = 현재)", "range": [-IDLE_SPAN, 2],
            "end": 0, "milestones": [], "action": None, "ack": None, "resumed": None, "alerts": [], "kind": None, "baseline": None,
            "verdicts": [verdict_row(e, when, now) for e in events if e["type"] == "diagnosis" and e["t"] >= lo],
            "paths": [{"key": k, "name": n, "before": None, "after": None} for k, n in PATH_NAMES.items()],
            "requests": [{"p": r["path"], "s": round(r["start"] - now, 3),
                          "e": round(r["t"] - now, 3) if r["t"] is not None else None, "r": r["result"], "ms": r["ms"]}
                         for r in requests.values() if r["start"] >= lo]}


def live():
    now = time.time()
    with lock:
        refresh(now)
        view = None
        opened = [e for e in events if e["type"] == "incident"]
        if opened:
            last = opened[-1]
            mine = [e for e in events if e.get("incident") == last["incident"]]
            # 복구로 끝났거나 감시가 재개된 사건은 잠시 더 보여 주고 내린다. 이관된 채 계속되는 사건은 계속 보여 준다.
            # 감시가 재개된 뒤에도 옛 사건 그림을 오래 붙들고 있으면 "아직 안 나았다"로 보이므로 짧게만 둔다.
            done = next((e for e in mine if e["type"] in ("resolved", "monitoring_resumed", "closed")), None)
            keep = KEEP_AFTER_END if done and done["type"] == "resolved" else KEEP_AFTER_RESUME
            if done is None or now - done["t"] < keep:
                view = build(with_baselines(), list(requests.values()), last["incident"], now=now)
                view["now"] = round(now - view["origin_t"], 3)
        if view is None:
            view = idle_view(now)
            view["now"] = 0
        by_agent = [e for e in events if e["type"] != "acknowledged"]   # 화면이 쓴 줄은 에이전트가 살아 있다는 증거가 아니다
        judged = [e for e in events if e["type"] in ("diagnosis", "verification")]
        shown = view["id"]
        waiting = [{"id": x["id"], "fault": x["fault"], "kind": x["kind"], "reason": x["reason"], "resumed": x["resumed"],
                    "escalated_at": time.strftime("%H:%M:%S", time.localtime(x["escalated_t"]))}
                   for x in escalations.values() if x["escalated_t"] and not x["acknowledged"] and x["id"] != shown]
        return {"mode": "live", "baseline": baselines[-1]["baseline"] if baselines else None, "incident": view,
                "waiting": waiting,   # 화면에 떠 있는 사건 말고, 아직 인수하지 않은 이관 사건
                "agent_age": round(now - by_agent[-1]["t"], 1) if by_agent else None,
                "model_down": bool(judged) and judged[-1]["verdict"]["by"] == "rule_fallback",
                "operator_token_required": bool(OPERATOR_TOKEN),
                "clock": time.strftime("%H:%M:%S", time.localtime(now))}


def acknowledge(body):
    if not isinstance(body, dict) or not isinstance(body.get("incident"), str):
        return 400, {"error": "incident가 필요합니다"}
    if OPERATOR_TOKEN and body.get("token") != OPERATOR_TOKEN:
        return 403, {"error": "담당자 권한이 없습니다"}
    with lock:   # 읽고, 확인하고, 쓰는 것을 한 번에: 두 사람이 동시에 눌러도 한 줄만 남는다
        refresh(time.time())
        x = escalations.get(body["incident"])
        if not x or not x["escalated_t"]:
            return 400, {"error": "이관된 사건이 아닙니다"}
        if x["acknowledged"]:
            return 200, {"ok": True, "already": True}
        # 감시가 재개된 사건도 인수할 수 있다: 감시 재개는 사람이 처리를 끝냈다는 뜻이 아니다
        with open(EVENTS, "a") as f:   # 에이전트도 같은 파일에 한 줄씩 덧붙여 쓴다
            f.write(json.dumps({"t": round(time.time(), 3), "type": "acknowledged", "incident": body["incident"],
                                "by": "web"}, ensure_ascii=False) + "\n")
        refresh(time.time())
    return 200, {"ok": True}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=HERE, **kwargs)

    def _json(self, code, body):
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.split("?")[0] == "/api/live":
            return self._json(200, live())
        return super().do_GET()

    def do_POST(self):
        if self.path != "/api/ack":
            return self._json(404, {"error": "not found"})
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        except ValueError:
            return self._json(400, {"error": "JSON 본문이 필요합니다"})
        return self._json(*acknowledge(body))

    def end_headers(self):
        if not self.path.startswith("/api/"):
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"web server on http://{HOST}:{PORT}" + (" (인수는 담당자 토큰 필요)" if OPERATOR_TOKEN else ""), flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
