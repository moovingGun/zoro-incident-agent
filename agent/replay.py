"""기록된 실제 창을 다시 물어 질문 문구를 비교한다 (서비스는 건드리지 않는다).

logs/agent/events.jsonl의 진단 창을 그대로 Zoro-1에 다시 넣고, 문구 변형별로 결과를 요약한다.
창의 기준 분류는 규칙 판정(normal / app_down / slow)을 쓴다.

    .venv/bin/python agent/replay.py        # zoro_server.py가 떠 있어야 한다
"""
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from agent import CONF, DIAG_Q, ZORO_URL  # noqa: E402
from observe import ROOT  # noqa: E402


def variant(slow_text=None, drop_unknown=False):
    q = json.loads(json.dumps(DIAG_Q))
    c = q["fault"]["criteria"]
    if slow_text:
        c["slow"] = slow_text
    if drop_unknown:
        del c["unknown"]
    return q


SIMPLE = "the app is reachable, but at least one path is timing out or much slower than the baseline"
VARIANTS = {
    "A 현재 문구": variant(),
    "B 지연 설명 단순화": variant(SIMPLE),
    "C B + '판단 불가' 제거": variant(SIMPLE, drop_unknown=True),
}


def ask(state, questions):
    body = json.dumps({"state": state, "questions": questions}).encode()
    req = urllib.request.Request(ZORO_URL, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        a = json.loads(r.read())["answers"]["fault"]
    return a["choice"], a["probabilities"][a["choice"]]


rows = [json.loads(x) for x in open(os.path.join(ROOT, "logs/agent/events.jsonl"))]
base = [r for r in rows if r["type"] == "baseline"][-1]["baseline"]
windows = [(r["verdict"]["rule"], r["window"]) for r in rows if r["type"] == "diagnosis"]
groups = sorted({g for g, _ in windows})
print(f"창 {len(windows)}개: " + ", ".join(f"{g} {sum(x == g for x, _ in windows)}" for g in groups))

for name, q in VARIANTS.items():
    res = [(g, *ask({"normal_baseline": base, "window_5s": w}, q)) for g, w in windows]
    print(f"\n{name}")
    for g in groups:
        mine = [(lab, p) for gg, lab, p in res if gg == g]
        hit = [p for lab, p in mine if lab == g]
        ok = sum(p >= CONF for p in hit)
        mid = sorted(hit)[len(hit) // 2] if hit else None
        wrong = {}
        for lab, p in mine:
            if lab != g and p >= CONF:
                wrong[lab] = wrong.get(lab, 0) + 1
        print(f"  {g:9s} 같은 답 {len(hit)}/{len(mine)}, 그중 확률 {CONF} 이상 {ok}, 중앙값 {mid}, "
              f"다른 답을 {CONF} 이상으로: {wrong or '없음'}")
