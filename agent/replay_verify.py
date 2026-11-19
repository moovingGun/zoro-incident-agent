"""기록된 사건의 조치 후 검증을, 지금의 검증 규칙(agent.check_verdict)으로 다시 돌려 본다. 서비스는 건드리지 않는다.

1부 기록 그대로: 당시 기록된 검증 판정을 순서대로 새 규칙에 넣는다. 기록은 당시 사건이 끝난 시점에서 멈춘다.
2부 이어 보기: 그 뒤는 정상 클라이언트 요청 기록으로 조치 후 집계를 다시 만들어 Zoro-1에 새로 묻는다.
   새로 묻기 전에, 기록된 시점들을 같은 방법으로 다시 만들어 당시 판정이 재현되는지부터 확인한다.

    .venv/bin/python agent/replay_verify.py 1119-183101 1119-185653     # zoro_server.py가 떠 있어야 한다
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import agent as A  # noqa: E402
from observe import PATHS, ROOT, p95  # noqa: E402

KO = {"recovered": "복구", "not_recovered": "미복구", "insufficient_data": "관측 부족"}


def load(path):
    return [json.loads(x) for x in open(os.path.join(ROOT, path))]


def requests_between(lo, hi):
    reqs = {}
    for r in load("logs/client/requests.jsonl"):
        if r["ev"] == "start" and lo <= r["t"] <= hi:
            reqs[r["id"]] = {"path": r["path"], "start": r["t"], "end": None}
        elif r["ev"] == "end" and r["id"] in reqs:
            reqs[r["id"]].update(end=r["t"], result=r["result"], ms=r["ms"])
    return list(reqs.values())


def since(reqs, t0, now):
    """Observer.since와 같은 집계를, 시각 now 시점 기준으로 다시 만든다."""
    mine = [r for r in reqs if t0 <= r["start"] <= now]
    done = [r for r in mine if r["end"] is not None and r["end"] <= now]
    out = {"completed_requests": len(done), "in_flight_requests": len(mine) - len(done)}
    for p in PATHS:
        d = [r for r in done if r["path"] == p]
        out[p + "_path"] = {"success": sum(r["result"] == "success" for r in d),
                            "failed": sum(r["result"] == "failed" for r in d),
                            "timeout": sum(r["result"] == "timeout" for r in d),
                            "p95_latency_ms": p95([r["ms"] for r in d]),
                            "in_flight": sum(1 for r in mine if r["path"] == p and r not in done)}
    return out


def step(inc, reqs, v, enough, now, action_t):
    new = sum(1 for r in reqs if r["end"] is not None and inc["last_check_t"] < r["end"] <= now
              and r["start"] >= action_t)
    result = A.check_verdict(inc, v, enough, new, now)
    note = ("확정: " + KO[result]) if result else "보류: 모델과 측정값이 다름" if inc["conflict"] else \
        "첫 판정, 한 번 더 확인" if inc["streak"] else "아직 판정 못 함"
    p = "규칙 대체" if v["p"] is None else f"{v['p']:.2f}"
    print(f"  조치 후 {now - action_t:5.1f}초  모델 {KO[v['label']]:5s} {p}  측정값 {KO[v['rule']]:5s}  -> {note}")
    return result


def replay(incident_id, events):
    mine = [e for e in events if e.get("incident") == incident_id]
    opened = next(e for e in mine if e["type"] == "incident")
    done = next(e for e in mine if e["type"] == "action_done")
    ended = next(e for e in mine if e["type"] in ("resolved", "escalated"))
    base = [e for e in events if e["type"] == "baseline" and e["t"] <= opened["t"]][-1]["baseline"]
    recorded = [e for e in mine if e["type"] == "verification"]
    action_t = done["t"]
    reqs = requests_between(action_t - 5, action_t + A.VERIFY_TIMEOUT + 10)
    print(f"\n=== 사건 {incident_id}: 진단 {opened['fault']}, 조치 {done['action']}, 당시 결말 "
          f"{'복구 확인' if ended['type'] == 'resolved' else '이관 (' + ended.get('reason', '') + ')'} ===")

    print("[1부] 기록된 판정을 새 규칙에 그대로 넣음")
    inc = {"streak": None, "last_check_t": action_t, "checks": 0, "conflicts": 0, "conflict": False}
    result = None
    for e in recorded:
        result = step(inc, reqs, e["verdict"], e["after"]["enough_samples"], e["t"], action_t)
        if result:
            break
    if result:
        print(f"  => 기록 안에서 결말: {KO[result]}")
        return
    print(f"  => 기록이 조치 후 {recorded[-1]['t'] - action_t:.1f}초에서 끝남. 여기까지는 결말 미정 "
          f"(검증 {inc['checks']}회 중 불일치 {inc['conflicts']}회)")

    print("[2부] 요청 기록으로 상태를 다시 만들어 Zoro-1에 새로 물음")
    same = 0
    for e in recorded:   # 재현 확인: 기록된 시점의 상태와 판정이 다시 나오는가
        after = since(reqs, action_t, e["t"])
        state = A.verify_request_state(done["action"], base, after)
        v = A.judge(state, A.VERIFY_Q, A.rule_verify(after, base))
        ok = state["after_action"] == e["after"] and v["label"] == e["verdict"]["label"] and \
            v["p"] is not None and abs(v["p"] - e["verdict"]["p"]) < 0.005
        same += ok
    print(f"  재현 확인: 기록된 {len(recorded)}개 시점 중 {same}개에서 집계와 판정이 당시와 같음")

    now = recorded[-1]["t"]
    while True:
        now += A.TICK
        after = since(reqs, action_t, now)
        state = A.verify_request_state(done["action"], base, after)
        v = A.judge(state, A.VERIFY_Q, A.rule_verify(after, base))
        if v["by"] != "zoro":
            sys.exit("  모델 서버에 물을 수 없습니다 (zoro_server.py 확인)")
        result = step(inc, reqs, v, state["after_action"]["enough_samples"], now, action_t)
        if result:
            print(f"  => 결말: {KO[result]} (조치 후 {now - action_t:.1f}초, 검증 {inc['checks']}회 중 불일치 {inc['conflicts']}회)")
            return
        if now - action_t > A.VERIFY_TIMEOUT:
            kind = "판정 불일치 이관" if inc["conflict"] else "판정 미확정 이관 (관측 부족)"
            print(f"  => 결말: {kind} (제한 시간 {A.VERIFY_TIMEOUT}초, 검증 {inc['checks']}회 중 불일치 {inc['conflicts']}회)")
            return


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    all_events = load("logs/agent/events.jsonl")
    for incident in sys.argv[1:]:
        replay(incident, all_events)
