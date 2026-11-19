"""Zoro-1 로컬 판정 테스트.

1) 진단: 패킷 통계(5초 창)를 주고 장애 유형을 고르게 한다.
2) 검증: 재시작 명령은 똑같이 성공했지만 결과가 다른 사건들을 구분하는지 본다.
   같은 사건을 원본 수치만 준 경우(raw)와 코드로 비율·충족 여부를 미리 계산해 준 경우(derived)로 비교한다.

실행: .venv/bin/python zoro_test.py
"""
import json
import sys
import time

MODEL_DIR = "models/Zoro-1"
sys.path.insert(0, MODEL_DIR)
from zoro_decide import Decider  # noqa: E402

t0 = time.perf_counter()
d = Decider(MODEL_DIR)
print(f"load: {time.perf_counter() - t0:.1f}s on {d.device}")
d.decide({"state": "warmup", "questions": {"q": {"type": "noul"}}})  # 첫 호출은 느리므로 측정에서 제외

# ---- 1) 진단 ---------------------------------------------------------------------------------------
BASELINE = {"syn_per_s": 40, "rst_ratio": 0.01, "retransmit_ratio": 0.005, "p95_latency_ms": 35,
            "completed_requests_per_s": 38, "http_5xx_ratio": 0.0, "top_src_ip_share": 0.08}

# 이름은 기대 정답. 마지막 것은 일부러 애매하게 만든 창.
CASES = {
    "normal": {"syn_per_s": 43, "rst_ratio": 0.012, "retransmit_ratio": 0.006, "p95_latency_ms": 38,
               "completed_requests_per_s": 41, "http_5xx_ratio": 0.0, "top_src_ip_share": 0.09},
    "app_down": {"syn_per_s": 41, "rst_ratio": 0.94, "retransmit_ratio": 0.01, "p95_latency_ms": None,
                 "completed_requests_per_s": 0, "http_5xx_ratio": 0.0, "top_src_ip_share": 0.08},
    "traffic_flood": {"syn_per_s": 4800, "rst_ratio": 0.05, "retransmit_ratio": 0.12, "p95_latency_ms": 900,
                      "completed_requests_per_s": 30, "http_5xx_ratio": 0.02, "top_src_ip_share": 0.97},
    "slow_response": {"syn_per_s": 39, "rst_ratio": 0.02, "retransmit_ratio": 0.09, "p95_latency_ms": 2600,
                      "completed_requests_per_s": 9, "http_5xx_ratio": 0.04, "top_src_ip_share": 0.08},
    "(ambiguous)": {"syn_per_s": 95, "rst_ratio": 0.04, "retransmit_ratio": 0.02, "p95_latency_ms": 140,
                    "completed_requests_per_s": 80, "http_5xx_ratio": 0.01, "top_src_ip_share": 0.22},
}

QUESTIONS = {
    "fault": {
        "type": "choice",
        "instructions": "Compare the current 5-second traffic window with the baseline. What is the state of the service?",
        "criteria": {
            "normal": "metrics are close to the baseline",
            "app_down": "the backend refuses connections: resets dominate and almost no request completes",
            "traffic_flood": "a huge spike of new connections, mostly from a single source IP",
            "slow_response": "connections are accepted at a normal rate but responses are very slow",
            "unknown": "abnormal, but none of the patterns above",
        },
    },
    "act": {"type": "noul", "instructions": "Should an automatic recovery action run right now?"},
    "severity": {"type": "score", "instructions": "How bad is the impact on users?",
                 "criteria": ["no impact", "degraded", "outage"]},
}

def vs_baseline(window):   # 산수는 코드가 미리 해 둔다: 각 지표가 평소의 몇 배인지
    return {k: f"{v / BASELINE[k]:.1f}x" for k, v in window.items() if v is not None and BASELINE[k] > 0}


for derived in (False, True):
    print(f"\n=== diagnose ({'derived' if derived else 'raw'}) ===")
    for name, window in CASES.items():
        state = {"baseline": BASELINE, "current_window": window}
        if derived:
            state["current_vs_baseline"] = vs_baseline(window)
        out = d.decide({"state": state, "questions": QUESTIONS})
        a = out["answers"]
        probs = {k: v for k, v in a["fault"]["probabilities"].items() if v >= 0.01}
        mark = "   " if name.startswith("(") else ("OK " if a["fault"]["choice"] == name else "BAD")
        print(f"{mark} [{name}] {out['timing_ms']} ms  fault={a['fault']['choice']} {json.dumps(probs)}"
              f"  act={a['act']['noul']}  severity={a['severity']['score']}")

# ---- 2) 검증 ---------------------------------------------------------------------------------------
# 조치가 끝난 뒤 시작한 정상 클라이언트 요청만 집계한다. in_flight = 아직 응답을 기다리는 요청.
NORMAL = {"db_path_p95_ms": 45, "light_path_p95_ms": 8}
MIN_SAMPLES = 20

# (기대 판정, 완료 수, 진행 중, DB 경로 성공률/p95, 경량 경로 성공률/p95)
VERIFY_CASES = [
    ("insufficient_data", 2, 8, 1.0, 60, 1.0, 10),      # 성공 2건, 아직 대부분 진행 중
    ("not_recovered", 30, 1, 1.0, 190, 1.0, 9),         # 부분 회복: 성공률은 돌아왔지만 평소의 4배로 느림
    ("not_recovered", 28, 6, 0.60, 3050, 1.0, 8),       # DB 경로 지연 지속 (시나리오 3)
    ("recovered", 32, 0, 1.0, 47, 1.0, 9),              # 회복 (시나리오 1)
]

VERIFY_Q = {"verdict": {
    "type": "choice",
    "instructions": "The app was restarted and the restart command succeeded. Judging only by requests from normal "
                    "users that started after the restart, did the service recover?",
    "criteria": {
        "recovered": "enough samples, requests succeed, and latency is back near the normal baseline on every path",
        "not_recovered": "enough samples, but some path is still failing or still much slower than the normal baseline",
        "insufficient_data": "too few completed requests after the action to tell yet",
    }}}


def verify_state(done, flying, db_ok, db_p95, light_ok, light_p95, derived):
    after = {"completed_requests": done, "in_flight_requests": flying,
             "db_path": {"success_rate": db_ok, "p95_latency_ms": db_p95},
             "light_path": {"success_rate": light_ok, "p95_latency_ms": light_p95}}
    if derived:   # 산수는 코드가 미리 해 둔다
        after["enough_samples"] = done >= MIN_SAMPLES
        after["db_path"]["latency_vs_normal"] = f"{db_p95 / NORMAL['db_path_p95_ms']:.1f}x"
        after["light_path"]["latency_vs_normal"] = f"{light_p95 / NORMAL['light_path_p95_ms']:.1f}x"
    return {"action": "restart_app", "normal_baseline": NORMAL, "min_samples_required": MIN_SAMPLES,
            "after_action": after}


for derived in (False, True):
    print(f"\n=== verify ({'derived' if derived else 'raw'}) ===")
    for expect, *obs in VERIFY_CASES:
        out = d.decide({"state": verify_state(*obs, derived), "questions": VERIFY_Q})
        v = out["answers"]["verdict"]
        mark = "OK " if v["choice"] == expect else "BAD"
        print(f"{mark} expect={expect:17s} got={v['choice']:17s} {out['timing_ms']} ms  {json.dumps(v['probabilities'])}")
