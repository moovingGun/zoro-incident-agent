"""판정 로직 점검. 서비스에 손대지 않고 검증 함수에 관측을 직접 넣어 본다.

    .venv/bin/python agent/check_logic.py

같은 관측으로 검증을 두 번 했을 때(그 사이 새 요청은 충분) 결론이 무엇인지 본다.
모델이 recovered를 0.9로 답한 경우와, 모델 호출이 실패해 규칙 판정을 쓰는 경우를 각각 확인한다.
"""
import sys

import agent as A

BASE = {"db_path_p95_ms": 44.5, "light_path_p95_ms": 4.1}


def path(success=0, timeout=0, p95=None, in_flight=0):
    return {"success": success, "failed": 0, "timeout": timeout, "p95_latency_ms": p95, "in_flight": in_flight}


def after(db, light):
    done = sum(p["success"] + p["failed"] + p["timeout"] for p in (db, light))
    return {"completed_requests": done, "in_flight_requests": db["in_flight"] + light["in_flight"],
            "db_path": db, "light_path": light}


def conclusion(aft, model_says=None):
    state = A.verify_request_state("restart_app", BASE, aft)
    rule = A.rule_verify(aft, BASE)
    v = ({"label": model_says, "p": 0.9, "by": "zoro", "rule": rule} if model_says
         else {"label": rule, "p": None, "by": "rule_fallback", "rule": rule})
    inc = {"streak": None, "checks": 0, "conflicts": 0, "last_check_t": 0}
    out = None
    for now in (1, 2):
        out = A.check_verdict(inc, v, state["after_action"]["enough_samples"], A.MIN_NEW, now)
    return out, rule, state


CASES = [   # (설명, 조치 후 관측, 기대하는 결론)
    ("주문 경로 완료 0건·대기 8건, ping만 20건 성공", after(path(in_flight=8), path(20, p95=4.0)), None),
    ("주문 경로 완료 3건(경로별 기준 미만), ping 20건 성공", after(path(3, p95=42.0, in_flight=5), path(20, p95=4.0)), None),
    ("두 경로 모두 12건씩 성공", after(path(12, p95=42.0), path(12, p95=4.0)), "recovered"),
    ("주문 경로 12건 타임아웃, ping 12건 성공", after(path(timeout=12, p95=2004.0, in_flight=8), path(12, p95=4.0)),
     "not_recovered"),
]

failed = 0
for name, aft, want in CASES:
    for label, says in (("모델이 recovered 0.9", "recovered"), ("모델 호출 실패(규칙 대체)", None)):
        if want == "not_recovered" and says:
            says, label = "not_recovered", "모델이 not_recovered 0.9"
        got, rule, state = conclusion(aft, says)
        ok = got == want
        failed += not ok
        print(f"{'통과' if ok else '결함'} | {name} | {label} -> 결론 {got} (기대 {want}, 규칙 판정 {rule}, "
              f"enough_samples {state['after_action']['enough_samples']})")

# 참고: 첫 사례를 실제 Zoro-1에 물으면 무엇이라 답하는가 (서버가 꺼져 있으면 규칙 대체로 나온다)
_, rule, state = conclusion(CASES[0][1])
v = A.judge(state, A.VERIFY_Q, rule)
print(f"참고 | 첫 사례에 대한 실제 응답: {v['label']} " + (f"{v['p']:.2f} (Zoro-1)" if v["by"] == "zoro" else "(규칙 대체)"))
sys.exit(1 if failed else 0)
