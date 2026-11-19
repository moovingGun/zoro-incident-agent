"""Zoro-1 진단 재테스트: 기획안의 관측 형식(경로별 정상 사용자 기록 + 구간별 패킷 통계)으로 묻는다.

장애 유형마다 강한 사례와 약한 사례를 하나씩 둔다. 진단과 다음 조치를 한 번의 호출로 함께 묻는다.

실행: .venv/bin/python zoro_diag_test.py
"""
import json
import sys
import time

MODEL_DIR = "models/Zoro-1"
sys.path.insert(0, MODEL_DIR)
from zoro_decide import Decider  # noqa: E402

BASELINE = {"requests_per_s": 40, "top_source_share": 0.10, "db_path_p95_ms": 45, "light_path_p95_ms": 8}


def window(rps, s5xx, no_resp, top, conn_err, db, light):
    """5초 창. db / light = 정상 사용자 요청의 (성공, 실패, 타임아웃, p95 ms)."""
    path = lambda t: {"success": t[0], "failed": t[1], "timeout": t[2], "p95_latency_ms": t[3]}  # noqa: E731
    return {"client_to_nginx": {"requests_per_s": rps, "status_5xx_share": s5xx, "no_response_share": no_resp,
                                "top_source_share": top},
            "nginx_to_app": {"connection_error_share": conn_err},
            "normal_user": {"db_path": path(db), "light_path": path(light)}}


# (기대 유형, 설명, 창)
CASES = [
    ("normal", "평소", window(41, 0.0, 0.0, 0.11, 0.0, (20, 0, 0, 47), (20, 0, 0, 9))),
    ("normal", "바쁘지만 정상", window(58, 0.005, 0.0, 0.14, 0.0, (29, 0, 0, 62), (29, 0, 0, 11))),
    ("app_down", "완전 다운", window(40, 0.98, 0.0, 0.10, 0.99, (0, 20, 0, None), (0, 20, 0, None))),
    ("app_down", "다운 2초 뒤(창의 40%)", window(40, 0.45, 0.0, 0.10, 0.46, (11, 9, 0, 46), (11, 9, 0, 8))),
    ("source_flood", "심한 폭주", window(3900, 0.03, 0.05, 0.96, 0.0, (14, 1, 5, 820), (17, 0, 3, 640))),
    ("source_flood", "중간 폭주", window(900, 0.0, 0.0, 0.82, 0.0, (20, 0, 0, 310), (20, 0, 0, 150))),
    ("dependency_slow", "DB 경로 심한 지연", window(38, 0.06, 0.08, 0.10, 0.0, (12, 1, 7, 2900), (20, 0, 0, 9))),
    ("dependency_slow", "DB 경로 중간 지연", window(40, 0.0, 0.0, 0.11, 0.0, (20, 0, 0, 600), (20, 0, 0, 8))),
    ("service_slow", "두 경로 심한 지연", window(39, 0.04, 0.10, 0.10, 0.0, (11, 1, 8, 2600), (13, 0, 7, 2300))),
    ("service_slow", "두 경로 중간 지연", window(40, 0.0, 0.0, 0.10, 0.0, (20, 0, 0, 700), (20, 0, 0, 520))),
    (None, "애매함", window(70, 0.01, 0.0, 0.25, 0.0, (20, 0, 0, 95), (20, 0, 0, 14))),
]

QUESTIONS = {
    "fault": {
        "type": "choice",
        "instructions": "Compare this 5-second window with the normal baseline. What is the state of the service?",
        "criteria": {
            "normal": "normal users' requests succeed and latency is near the baseline on both paths",
            "app_down": "nginx cannot reach the app: connection errors to the app and 5xx responses on both paths",
            "source_flood": "request volume is far above the baseline and one source sends most of it",
            "dependency_slow": "only the path that uses the database is slow or timing out; the light path is still normal",
            "service_slow": "both paths are slow or timing out, with no connection errors and no dominant source",
            "unknown": "abnormal, but none of the patterns above",
        },
    },
    "action": {
        "type": "choice",
        "instructions": "Nothing has been tried yet in this incident. Which of the allowed actions should run next?",
        "criteria": {
            "observe": "keep watching, take no action yet",
            "restart_app": "restart the main app container",
            "limit_top_source": "temporarily rate-limit the source that sends the most requests",
            "escalate": "hand off to the on-call person with the observations",
        },
    },
}

t0 = time.perf_counter()
d = Decider(MODEL_DIR)
print(f"load: {time.perf_counter() - t0:.1f}s on {d.device}")
d.decide({"state": "warmup", "questions": {"q": {"type": "noul"}}})

def with_ratios(win):   # 경로별 지연이 평소의 몇 배인지 코드가 미리 계산
    win = json.loads(json.dumps(win))
    for p in ("db_path", "light_path"):
        v = win["normal_user"][p]["p95_latency_ms"]
        if v is not None:
            win["normal_user"][p]["latency_vs_normal"] = f"{v / BASELINE[p + '_p95_ms']:.1f}x"
    return win


def merged_questions():   # 두 지연 유형을 하나로 합치고, 첫 조치 후보에서 이관을 뺀다
    q = json.loads(json.dumps(QUESTIONS))
    c = q["fault"]["criteria"]
    q["fault"]["criteria"] = {"normal": c["normal"], "app_down": c["app_down"], "source_flood": c["source_flood"],
                              "slow": "requests are accepted but at least one path is much slower than the baseline "
                                      "or timing out, with no connection errors and no dominant source",
                              "unknown": c["unknown"]}
    del q["action"]["criteria"]["escalate"]
    return q


VARIANTS = {"raw": (lambda w: w, QUESTIONS, {}),
            "ratios": (with_ratios, QUESTIONS, {}),
            "merged": (lambda w: w, merged_questions(), {"dependency_slow": "slow", "service_slow": "slow"})}

for vname, (prep, questions, rename) in VARIANTS.items():
    print(f"\n=== {vname} ===")
    right = total = 0
    for expect, label, win in CASES:
        expect = rename.get(expect, expect)
        out = d.decide({"state": {"normal_baseline": BASELINE, "window_5s": prep(win)}, "questions": questions})
        f, a = out["answers"]["fault"], out["answers"]["action"]
        probs = {k: v for k, v in f["probabilities"].items() if v >= 0.05}
        if expect is None:
            mark = "   "
        else:
            total += 1
            right += f["choice"] == expect
            mark = "OK " if f["choice"] == expect else "BAD"
        print(f"{mark} {label:20s} expect={str(expect):16s} got={f['choice']:16s} {json.dumps(probs)}")
        print(f"      action={a['choice']} ({max(a['probabilities'].values())})  {out['timing_ms']} ms")
    print(f"fault: {right}/{total} correct")
