"""Zoro-1 조치 선택 테스트: 운영 지침(runbook)을 상태에 글로 넣으면 그에 맞는 조치를 고르는지 본다.

세 가지로 비교한다.
  none: 지침 없음 (대조군)
  A:    지연이면 앱을 한 번 재시작
  B:    지연이면 재시작하지 말고 바로 이관  (A와 지연 항목만 다름: 모델이 글을 실제로 읽는지 확인)
이미 시도한 조치가 있는 두 번째 단계도 묻는다.

실행: .venv/bin/python zoro_runbook_test.py
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


W = {
    "normal": window(41, 0.0, 0.0, 0.11, 0.0, (20, 0, 0, 47), (20, 0, 0, 9)),
    "app_down": window(40, 0.98, 0.0, 0.10, 0.99, (0, 20, 0, None), (0, 20, 0, None)),
    "app_down_early": window(40, 0.45, 0.0, 0.10, 0.46, (11, 9, 0, 46), (11, 9, 0, 8)),
    "flood": window(3900, 0.03, 0.05, 0.96, 0.0, (14, 1, 5, 820), (17, 0, 3, 640)),
    "db_slow": window(38, 0.06, 0.08, 0.10, 0.0, (12, 1, 7, 2900), (20, 0, 0, 9)),
    "all_slow": window(39, 0.04, 0.10, 0.10, 0.0, (11, 1, 8, 2600), (13, 0, 7, 2300)),
}

COMMON = ["If the app is down, restart the main app.",
          "If the app is still down after a restart, switch traffic to the standby app.",
          "If one source floods the service, rate-limit that source.",
          "If the service is normal, keep observing."]
LAST = ["Never repeat an action that was already tried in this incident.",
        "If no allowed action is left, escalate to the on-call person."]
RUNBOOKS = {
    "none": None,
    "A": COMMON + ["If responses are slow or timing out, restart the main app once. "
                   "A database restart is never automatic."] + LAST,
    "B": COMMON + ["If responses are slow or timing out, do not restart anything: "
                   "escalate to the on-call person right away."] + LAST,
}

# (창, 이미 시도한 조치, 지침별 기대 조치). none은 채점하지 않는다.
STEPS = [
    ("normal", [], {"A": "observe", "B": "observe"}),
    ("app_down", [], {"A": "restart_app", "B": "restart_app"}),
    ("app_down_early", [], {"A": "restart_app", "B": "restart_app"}),
    ("flood", [], {"A": "limit_top_source", "B": "limit_top_source"}),
    ("db_slow", [], {"A": "restart_app", "B": "escalate"}),
    ("all_slow", [], {"A": "restart_app", "B": "escalate"}),
    ("app_down", ["restart_app"], {"A": "switch_to_standby", "B": "switch_to_standby"}),
    ("db_slow", ["restart_app"], {"A": "escalate", "B": "escalate"}),
    ("flood", ["limit_top_source"], {"A": "escalate", "B": "escalate"}),
]

QUESTIONS = {
    "fault": {
        "type": "choice",
        "instructions": "Compare this 5-second window with the normal baseline. What is the state of the service?",
        "criteria": {
            "normal": "normal users' requests succeed and latency is near the baseline on both paths",
            "app_down": "nginx cannot reach the app: connection errors to the app and 5xx responses on both paths",
            "source_flood": "request volume is far above the baseline and one source sends most of it",
            "slow": "requests are accepted but at least one path is much slower than the baseline or timing out, "
                    "with no connection errors and no dominant source",
            "unknown": "abnormal, but none of the patterns above",
        },
    },
    "action": {
        "type": "choice",
        "instructions": "Following the runbook (if one is given) and what was already tried in this incident, "
                        "which action should run next?",
        "criteria": {
            "observe": "keep watching, take no action yet",
            "restart_app": "restart the main app container",
            "switch_to_standby": "route traffic to the standby app",
            "limit_top_source": "temporarily rate-limit the source that sends the most requests",
            "escalate": "hand off to the on-call person with the observations",
        },
    },
}

t0 = time.perf_counter()
d = Decider(MODEL_DIR)
print(f"load: {time.perf_counter() - t0:.1f}s on {d.device}")
d.decide({"state": "warmup", "questions": {"q": {"type": "noul"}}})

for rb_name, runbook in RUNBOOKS.items():
    print(f"\n=== runbook {rb_name} ===")
    right = total = 0
    for wname, tried, expect in STEPS:
        state = {"already_tried": tried, "normal_baseline": BASELINE, "window_5s": W[wname]}
        if runbook:
            state = {"runbook": runbook, **state}
        out = d.decide({"state": state, "questions": QUESTIONS})
        f, a = out["answers"]["fault"], out["answers"]["action"]
        top2 = dict(sorted(a["probabilities"].items(), key=lambda kv: -kv[1])[:2])
        want = expect.get(rb_name)
        if want is None:
            mark = "   "
        else:
            total += 1
            right += a["choice"] == want
            mark = "OK " if a["choice"] == want else "BAD"
        print(f"{mark} {wname:15s} tried={str(tried):22s} want={str(want):18s} got={a['choice']:18s} "
              f"{json.dumps(top2)}  fault={f['choice']}  {out['timing_ms']} ms")
    if total:
        print(f"action: {right}/{total} correct")
