"""관측 집계: 정상 클라이언트 기록과 nginx 접근 기록을 읽어 5초 창 통계를 만든다.

시계는 기록에 찍힌 시각을 그대로 쓴다 (컨테이너와 호스트의 시계 차이를 피하려고).
정상 클라이언트는 0.25초마다 시작 줄을 쓰므로 가장 최근 줄의 시각이 곧 현재 시각이다.

    .venv/bin/python agent/observe.py     # 1초마다 현재 창을 출력 (확인용)
"""
import json
import os
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENT_LOG = os.path.join(ROOT, "logs/client/requests.jsonl")
NGINX_LOG = os.path.join(ROOT, "logs/nginx/access.jsonl")
PATHS = ("db", "light")
KEEP = 120   # 초: 이보다 오래된 기록은 버린다


class Tail:
    """파일 끝에서부터 새로 추가되는 줄만 읽는다."""

    def __init__(self, path):
        self.f = open(path)
        self.f.seek(0, os.SEEK_END)
        self.buf = ""

    def read(self):
        self.buf += self.f.read()
        *lines, self.buf = self.buf.split("\n")
        return [json.loads(x) for x in lines if x]


def p95(values):
    if not values:
        return None
    s = sorted(values)
    return round(s[min(len(s) - 1, int(0.95 * len(s)))], 1)


class Observer:
    def __init__(self):
        self.client, self.nginx = Tail(CLIENT_LOG), Tail(NGINX_LOG)
        self.reqs = {}      # 요청 번호 -> {"path", "start", "end", "result", "ms"}
        self.hits = []      # nginx 줄
        self.now = 0.0      # 기록상의 현재 시각
        self.fresh = time.time()   # 마지막으로 새 줄을 본 호스트 시각 (관측 끊김 감지용)

    def poll(self):
        rows = self.client.read()
        for r in rows:
            if r["ev"] == "start":
                self.reqs[r["id"]] = {"path": r["path"], "start": r["t"], "end": None}
            elif r["id"] in self.reqs:
                self.reqs[r["id"]].update(end=r["t"], result=r["result"], ms=r["ms"])
            self.now = max(self.now, r["t"])
        self.hits += self.nginx.read()
        if rows:
            self.fresh = time.time()
        old = self.now - KEEP
        self.reqs = {k: v for k, v in self.reqs.items() if v["start"] >= old}
        self.hits = [h for h in self.hits if h["t"] >= old]

    def _path_stats(self, done, flying):
        out = {}
        for p in PATHS:
            d = [r for r in done if r["path"] == p]
            f = [r for r in flying if r["path"] == p]
            out[p + "_path"] = {
                "success": sum(r["result"] == "success" for r in d),
                "failed": sum(r["result"] == "failed" for r in d),
                "timeout": sum(r["result"] == "timeout" for r in d),
                "p95_latency_ms": p95([r["ms"] for r in d]),
                "in_flight": len(f),
            }
        return out

    def window(self, span=5.0):
        """최근 span초 창. 정상 사용자 통계는 이 창 안에서 끝난 요청 기준."""
        lo = self.now - span
        done = [r for r in self.reqs.values() if r["end"] and r["end"] >= lo]
        flying = [r for r in self.reqs.values() if not r["end"]]
        hits = [h for h in self.hits if h["t"] >= lo]
        n = len(hits) or 1
        by_src = {}
        for h in hits:
            by_src[h["src"]] = by_src.get(h["src"], 0) + 1
        top_src = max(by_src, key=by_src.get) if by_src else None
        return {
            "client_to_nginx": {
                "requests_per_s": round(len(hits) / span, 1),
                "status_5xx_share": round(sum(h["status"] >= 500 for h in hits) / n, 2),
                "no_response_share": round(sum(h["status"] == 499 for h in hits) / n, 2),   # 응답 전에 사용자가 끊음
                "top_source_share": round(by_src[top_src] / n, 2) if top_src else 0.0,
            },
            "nginx_to_app": {
                # 앱에 연결하지 못한 요청: 연결 시간 초과는 "-", 이름을 못 찾으면 ""
                "connection_error_share": round(sum(h["uct"] in ("-", "") for h in hits) / n, 2),
            },
            "normal_user": self._path_stats(done, flying),
        }, {"top_source": top_src, "ended": len(done)}

    def ended_since(self, t0, started_after=None):
        """t0 이후에 끝난 정상 사용자 요청 수 (판정 사이에 새 증거가 쌓였는지 확인용).
        started_after를 주면 그 뒤에 시작한 요청만 센다 (조치 전에 시작한 요청은 조치 후 증거가 아니다)."""
        return sum(1 for r in self.reqs.values() if r["end"] and r["end"] > t0
                   and (started_after is None or r["start"] >= started_after))

    def since(self, t0):
        """t0 이후에 시작한 정상 사용자 요청만 집계 (조치 후 검증용)."""
        mine = [r for r in self.reqs.values() if r["start"] >= t0]
        done = [r for r in mine if r["end"]]
        flying = [r for r in mine if not r["end"]]
        return {"completed_requests": len(done), "in_flight_requests": len(flying), **self._path_stats(done, flying)}


if __name__ == "__main__":
    o = Observer()
    while True:
        time.sleep(1)
        o.poll()
        w, _ = o.window()
        u = w["normal_user"]
        fmt = lambda s: f"ok={s['success']:2d} fail={s['failed']:2d} to={s['timeout']:2d} fly={s['in_flight']:2d} p95={s['p95_latency_ms']}"  # noqa: E731
        print(f"{time.strftime('%H:%M:%S')} nginx={json.dumps(w['client_to_nginx'])} conn_err={w['nginx_to_app']['connection_error_share']}"
              f" | db[{fmt(u['db_path'])}] light[{fmt(u['light_path'])}]", flush=True)
