"""평소 기준값만 바꿔, 기록된 같은 창을 다시 판정해 본다. 에이전트 코드와 서비스는 건드리지 않는다.

고정한 입력 (19:37에 시작한 에이전트 실행)
  - 정상 구간: 장애가 주입돼 있지 않던 때 에이전트가 실제로 본 진단 창
  - 앱 다운: 주입 뒤 사건이 열리기까지의 진단 창, 재시작 뒤 30초 동안의 검증 집계
  - DB 지연: 같은 실행에서 주입 뒤의 진단 창(당시에는 이관 상태라 사건이 열리지 않았다),
             그리고 재시작까지 간 가장 가까운 실제 DB 지연 사건(19:43)의 검증 집계
바꾸는 것: normal_baseline 하나. 질문 문구와 기준 확률(0.7), 판정 규칙은 agent.py의 것을 그대로 쓴다.

기준값 후보: 에이전트가 실제로 쓴 10초 기준, 같은 시작 직전 구간에서 계산한 30초·60초 기준,
            비교용으로 18:27 실행의 기준.

    .venv/bin/python agent/replay_baseline.py        # zoro_server.py가 떠 있어야 한다
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import agent as A  # noqa: E402
from observe import PATHS, ROOT, p95  # noqa: E402

RUN_START = "19:37"          # 이 시각에 시작한 에이전트 실행
DB_INCIDENT = "1119-194305"  # 재시작까지 간 실제 DB 지연 사건
KO = {"recovered": "복구", "not_recovered": "미복구", "insufficient_data": "관측 부족"}
clock = lambda t: time.strftime("%H:%M:%S", time.localtime(t))  # noqa: E731


def load(path):
    return [json.loads(x) for x in open(os.path.join(ROOT, path))]


events, injections = load("logs/agent/events.jsonl"), load("logs/injections.jsonl")
ended = sorted((r for r in load("logs/client/requests.jsonl") if r["ev"] == "end"), key=lambda r: r["t"])
hits = load("logs/nginx/access.jsonl")
bases = [e for e in events if e["type"] == "baseline"]
start = next(b for b in bases if clock(b["t"]).startswith(RUN_START))
stop = next(b["t"] for b in bases if b["t"] > start["t"])
run = [e for e in events if start["t"] <= e["t"] < stop]


def baseline_from(lo, hi):
    """agent.measure_baseline과 같은 방식으로, 기록의 [lo, hi] 구간에서 평소 기준을 계산한다."""
    base = {}
    per_s = [sum(1 for h in hits if t <= h["t"] < t + 5) / 5 for t in range(int(lo), int(hi) - 4)]
    base["requests_per_s"] = sorted(per_s)[len(per_s) // 2]
    shares = []
    for t in range(int(lo), int(hi) - 4):
        win = [h["src"] for h in hits if t <= h["t"] < t + 5]
        shares.append(max(win.count(s) for s in set(win)) / len(win))
    base["top_source_share"] = round(sorted(shares)[len(shares) // 2], 2)
    for p in PATHS:
        base[p + "_path_p95_ms"] = p95([r["ms"] for r in ended if r["path"] == p and lo <= r["t"] <= hi])
    return base


def new_since(t0, t1, started_after=None):
    return sum(1 for r in ended if t0 < r["t"] <= t1 and (started_after is None or r["start"] >= started_after))


def monitor(windows, base, gate=True, stop_at_open=False):
    """진단 창을 순서대로 넣어 agent.py의 사건 개시 규칙을 따른다. -> (열린 사건 [(t, 유형)], 보류 수, 모델이 0.7 이상으로 이상이라 한 수)"""
    streak, streak_t, opened, held, alarms, prev = None, 0.0, [], 0, 0, None
    for e in windows:
        if prev is not None and e["t"] - prev > 2.5:   # 구간이 끊긴 곳에서는 연속 판정을 이어 세지 않는다
            streak = None
        prev = e["t"]
        w = e["window"]
        v = A.judge({"normal_baseline": base, "window_5s": w}, A.DIAG_Q, A.rule_diagnose(w, base))
        if v["by"] != "zoro":
            sys.exit("모델 서버에 물을 수 없습니다 (zoro_server.py 확인)")
        fault = v["label"] if v["label"] in A.POLICY and A.accepted(v) else None
        alarms += bool(fault)
        if fault and v["rule"] == "normal" and gate:
            held += 1
            fault = None
        new = new_since(streak_t, e["t"]) if streak else 0
        if not fault:
            streak = None
        elif streak != fault:
            streak, streak_t = fault, e["t"]
        elif new >= A.MIN_NEW:
            opened.append((e["t"], fault))
            streak = None
            if stop_at_open:
                break
    return opened, held, alarms


def verify(incident_id, base):
    """기록된 조치 후 집계를 순서대로 넣어 agent.check_verdict를 따른다. -> (결말, 조치 후 초, 검증 수, 불일치 수)"""
    mine = [e for e in events if e.get("incident") == incident_id]
    done = next(e for e in mine if e["type"] == "action_done")
    inc = {"streak": None, "last_check_t": done["t"], "checks": 0, "conflicts": 0, "conflict": False}
    checks = [e for e in mine if e["type"] == "verification"]
    for e in checks:
        state = A.verify_request_state(done["action"], base, e["counts"])
        v = A.judge(state, A.VERIFY_Q, A.rule_verify(e["counts"], base))
        new = new_since(inc["last_check_t"], e["t"], started_after=done["t"])
        result = A.check_verdict(inc, v, state["after_action"]["enough_samples"], new, e["t"])
        if result:
            return KO[result], e["t"] - done["t"], inc["checks"], inc["conflicts"]
        if e["t"] - done["t"] > A.VERIFY_TIMEOUT:
            return ("판정 불일치 이관" if inc["conflict"] else "판정 미확정 이관"), e["t"] - done["t"], inc["checks"], inc["conflicts"]
    return "기록이 끝나 결말 미정", checks[-1]["t"] - done["t"], inc["checks"], inc["conflicts"]


# ---- 고정 입력 --------------------------------------------------------------------------------------
faults, begun = [], None       # 장애가 주입돼 있던 구간 (초기화 뒤 창이 비워질 때까지 10초 더)
for i in injections:
    if i["inject"] != "reset" and begun is None:
        begun = i
    elif i["inject"] == "reset" and begun is not None:
        faults.append((begun["inject"], begun["t"], i["t"] + 10))
        begun = None
diag = [e for e in run if e["type"] == "diagnosis"]
app = next(e for e in run if e["type"] == "incident")
app_end = next(e for e in run if e["type"] == "escalated")
quiet = [e for e in diag if not any(a - 1 <= e["t"] <= b for _, a, b in faults) and not app["t"] - 6 <= e["t"] <= app_end["t"] + 6]
app_inj = next(t for k, t, _ in faults if k == "app_down" and start["t"] <= t < app["t"])
db_inj = next(t for k, t, _ in faults if k == "db_delay" and start["t"] <= t < stop)
app_diag = [e for e in diag if app_inj - 1 <= e["t"] <= app["t"]]
db_diag = [e for e in diag if db_inj - 1 <= e["t"] <= db_inj + 40]

t0 = start["t"]   # 에이전트가 기준 측정을 끝낸 시각. 그 직전 구간에서 길이만 달리해 기준을 계산한다
candidates = {
    "10초 (에이전트가 실제로 쓴 값)": start["baseline"],
    "10초 (같은 방법으로 다시 계산)": baseline_from(t0 - 10, t0),
    "30초": baseline_from(t0 - 30, t0),
    "60초": baseline_from(t0 - 60, t0),
    "비교용: 18:27 실행의 기준": next(b for b in bases if clock(b["t"]).startswith("18:27"))["baseline"],
}
print(f"에이전트 실행 {clock(start['t'])} ~ {clock(stop)}")
print(f"정상 구간 창 {len(quiet)}개 ({clock(quiet[0]['t'])} ~ {clock(quiet[-1]['t'])}, 끊긴 구간 포함)")
print(f"앱 다운: 주입 {clock(app_inj)}, 진단 창 {len(app_diag)}개, 사건 {app['incident']}의 검증 집계 "
      f"{sum(1 for e in run if e['type'] == 'verification')}개")
print(f"DB 지연: 주입 {clock(db_inj)}, 진단 창 {len(db_diag)}개 / 검증은 사건 {DB_INCIDENT}의 집계")
chunks = [baseline_from(t0 - 60 + 10 * k, t0 - 50 + 10 * k) for k in range(6)]
print("참고: 시작 직전 60초를 10초씩 나눠 계산한 DB 경로 p95 =", [c["db_path_p95_ms"] for c in chunks],
      "/ 경량 경로 p95 =", [c["light_path_p95_ms"] for c in chunks])

for name, base in candidates.items():
    print(f"\n=== 기준 {name}: DB {base['db_path_p95_ms']}ms / 경량 {base['light_path_p95_ms']}ms ===", flush=True)
    opened, held, alarms = monitor(quiet, base)
    ungated, _, _ = monitor(quiet, base, gate=False)
    print(f"정상 구간 {len(quiet)}초: 열린 사건 {len(opened)}건 (측정값 확인으로 보류 {held}회). "
          f"모델이 0.7 이상으로 이상이라 한 창 {alarms}개, 측정값 확인이 없었다면 열렸을 사건 {len(ungated)}건", flush=True)
    opened, _, _ = monitor(app_diag, base, stop_at_open=True)
    how = f"주입 {opened[0][0] - app_inj:.1f}초 뒤 {opened[0][1]} 사건 열림" if opened else "사건 안 열림"
    out, sec, n, bad = verify(app["incident"], base)
    print(f"앱 다운: {how} -> 재시작 후 {out} (조치 후 {sec:.1f}초, 검증 {n}회 중 불일치 보류 {bad}회)", flush=True)
    opened, _, _ = monitor(db_diag, base, stop_at_open=True)
    how = f"주입 {opened[0][0] - db_inj:.1f}초 뒤 {opened[0][1]} 사건 열림" if opened else "40초 안에 사건 안 열림"
    out, sec, n, bad = verify(DB_INCIDENT, base)
    print(f"DB 지연: {how} / 재시작 후 {out} (조치 후 {sec:.1f}초, 검증 {n}회 중 불일치 보류 {bad}회)", flush=True)
