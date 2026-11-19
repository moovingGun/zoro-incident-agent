"""runs/의 실제 실행 기록에서 재생 화면용 데이터(web/data/replay.json)를 만든다.

    .venv/bin/python web/build_replay.py

주입 기록에서 마지막 앱 다운과 마지막 DB 지연을 고른다. 각 주입 뒤, 다음 장애 주입 전에,
60초 안에 기대한 유형으로 열린 사건만 그 주입의 사건으로 인정한다. 없으면 만들지 않고 멈춘다.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from incident_view import build  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = "2026-11-19_first_integration"
TITLES = {"app_down": "앱 다운", "db_delay": "DB 지연"}
EXPECT = {"app_down": "app_down", "db_delay": "slow"}   # 주입 종류 -> 에이전트가 열어야 하는 사건 유형


def load(kind):
    return [json.loads(x) for x in open(os.path.join(ROOT, "runs", f"{RUN}_{kind}.jsonl"))]


events, injections, requests = load("events"), load("injections"), load("requests")
baseline = [e for e in events if e["type"] == "baseline"][-1]["baseline"]
incidents = []
for kind in TITLES:
    inj = [i for i in injections if i["inject"] == kind][-1]
    until = min((i["t"] for i in injections if i["t"] > inj["t"] and i["inject"] != "reset"), default=float("inf"))
    opened = next((e for e in events if e["type"] == "incident" and inj["t"] <= e["t"] < min(until, inj["t"] + 60)), None)
    if not opened or opened["fault"] != EXPECT[kind]:
        sys.exit(f"{kind}: 이 주입 뒤 60초 안에(다음 주입 전에) '{EXPECT[kind]}' 사건이 열린 기록이 없습니다. "
                 f"찾은 것: {opened and opened['fault']}")
    view = build(events, requests, opened["incident"], inj["t"], "주입")
    view.update(scenario=kind, title=TITLES[kind],   # 사건의 kind(이관 사유)를 덮어쓰지 않도록 다른 이름을 쓴다
                injected="주 앱 컨테이너를 강제 종료" if kind == "app_down" else f"DB 쿼리에 {inj['ms'] / 1000:g}초 지연을 주입",
                injected_at=time.strftime("%H:%M:%S", time.localtime(inj["t"])))
    incidents.append(view)
    print(kind, "->", view["outcome"], view["kind"] or "", [(m["label"], m["at"]) for m in view["milestones"]])
    assert view["kind"] in (None, "not_recovered", "disagreement", "undetermined", "no_action", "action_failed")
    for p in view["paths"]:
        print("   ", p["name"], "before", p["before"], "after", p["after"])

out = {"mode": "replay", "run": RUN, "recorded_on": "2026-11-19", "baseline": baseline,
       "sources": [f"runs/{RUN}_{k}.jsonl" for k in ("events", "injections", "requests")], "incidents": incidents}
os.makedirs(os.path.join(ROOT, "web/data"), exist_ok=True)
with open(os.path.join(ROOT, "web/data/replay.json"), "w") as f:
    json.dump(out, f, ensure_ascii=False)
print("wrote web/data/replay.json")
