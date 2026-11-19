"""Zoro-1 다음 단계 선택 테스트 v2 (dev / holdout).
실행: .venv/bin/python zoro_nextstep_test.py dev
      .venv/bin/python zoro_nextstep_test.py final <A|B|C>
관측 숫자는 기준선과 무관. 기준선만 바꿔서 같은 케이스를 3번 호출한다. 무작위 없음.
"""
import json
import statistics as st
import sys
import urllib.request
from collections import Counter, defaultdict

URL = "http://127.0.0.1:8765/decide"
MIN_SAMPLES = 20
DEV_BASES = [(45, 8), (41, 4), (33, 1.5)]
HOLD_BASES = [(38, 2.5), (43.5, 5.5), (52, 11)]

CTX = ("db_path is the orders API and reads the database; light_path is a ping API that does not use the database. "
       "The client timeout is 2 seconds, so a path with p95 around 2000 ms is timing out. ")
WORDINGS = {
    "A": {
        "s1": ("An incident was detected and the main app was restarted; the restart command succeeded. The state "
               "shows what happened to requests after the restart. " + CTX +
               "A standby app exists that uses the same database as the main app. Choose the single best next step.",
               {"observe_more": "Fewer than min_samples_required requests have finished since the restart, so there "
                                "is not enough evidence yet; wait for more requests to finish before deciding.",
                "probe_standby": "Enough requests have finished and db_path is still failing while light_path is "
                                 "fine; send the same db_path request to the standby app to learn whether the "
                                 "problem is only in the main app.",
                "escalate": "Hand the incident over to the on-call person."}),
        "s2": ("An incident was detected and the main app was restarted; the restart command succeeded. The main "
               "app is still failing on db_path. The same db_path request was then sent to the standby app, which "
               "uses the same database. " + CTX + "Choose the single best next step.",
               {"observe_more": "The standby probe result is not conclusive; wait for more results before deciding.",
                "switch_to_standby": "The standby app answers db_path requests successfully while the main app "
                                     "does not; route user traffic to the standby app.",
                "escalate": "The standby app fails db_path requests too, so switching would not help; hand the "
                            "incident over to the on-call person."}),
    },
    "C": {
        "s1": ("An incident was detected and the main app was restarted; the restart command succeeded. The state "
               "shows what happened to requests after the restart. " + CTX +
               "A standby app exists that uses the same database as the main app. First check enough_samples: if it "
               "is false, no conclusion can be drawn yet, whatever the finished requests show. Choose the single "
               "best next step.",
               {"observe_more": "enough_samples is false: too few requests have finished to judge anything, even if "
                                "some of the finished requests failed or timed out; wait for more requests to "
                                "finish before deciding.",
                "probe_standby": "enough_samples is true and db_path is still failing while light_path is fine; "
                                 "send the same db_path request to the standby app to learn whether the problem is "
                                 "only in the main app.",
                "escalate": "Hand the incident over to the on-call person."}),
        "s2": ("An incident was detected and the main app was restarted; the restart command succeeded. The main "
               "app is still failing on db_path. The same db_path request was then sent to the standby app, which "
               "uses the same database. " + CTX + "Choose the single best next step.",
               {"observe_more": "The standby probe result is not conclusive; wait for more results before deciding.",
                "switch_to_standby": "The standby app answers db_path requests successfully while the main app "
                                     "does not; route user traffic to the standby app.",
                "escalate": "The standby app fails db_path requests too, so switching would not help; hand the "
                            "incident over to the on-call person."}),
    },
    "B": {
        "s1": ("The main app was restarted after an incident and the restart worked. Below are the request results "
               "collected since then. " + CTX +
               "There is also a standby app connected to the same database. Pick what to do next.",
               {"observe_more": "The sample is still too small (completed_requests is below the minimum): keep "
                                "waiting for in-flight requests to finish.",
                "probe_standby": "The sample is big enough and the database path is still broken after the restart "
                                 "while the light path works: test the standby app with the same database request "
                                 "to see if only the main app is at fault.",
                "escalate": "Page the on-call engineer to take over."}),
        "s2": ("The main app was restarted after an incident and the restart worked, but the database path is still "
               "failing. A test was then run by sending the same database-path requests to the standby app, which "
               "shares the same database. " + CTX + "Pick what to do next.",
               {"observe_more": "The test is inconclusive: keep waiting for more results.",
                "switch_to_standby": "The test requests to the standby app succeeded: send users to the standby app "
                                     "instead of the main app.",
                "escalate": "The test requests to the standby app also failed, so the standby cannot take over: "
                            "page the on-call engineer."}),
    },
}


def build(action_base, db, light, probe=None):
    """db/light = (success, failed, timeout, p95, in_flight). baseline만 바깥에서 바뀐다."""
    paths = {}
    done = 0
    infl = 0
    for name, (s, f, t, p95, m) in (("db_path", db), ("light_path", light)):
        n = s + f + t
        done += n
        infl += m
        paths[name] = {"success_rate": round(s / n, 2) if n else None, "p95_latency_ms": p95, "timeouts": t,
                       "in_flight": m}
    state = {"action": "restart_app (command succeeded)",
             "normal_baseline": {"db_path_p95_ms": action_base[0], "light_path_p95_ms": action_base[1]},
             "min_samples_required": MIN_SAMPLES,
             "after_action": {"completed_requests": done, "in_flight_requests": infl,
                              "enough_samples": done >= MIN_SAMPLES, **paths}}
    if probe:
        n, s, p95 = probe
        state["standby_probe"] = {"path": "db_path", "requests_sent": n, "succeeded": s, "timed_out": n - s,
                                  "p95_latency_ms": p95}
    return state


# 기준선과 무관한 관측. (success, failed, timeout, p95, in_flight)
DEV_S1A = [("few-ok-1", (2, 0, 0, 44, 3), (3, 0, 0, 5, 2)),
           ("few-ok-2", (0, 0, 0, None, 4), (6, 0, 0, 4, 2)),
           ("few-bad-1", (0, 0, 3, 2004, 4), (4, 0, 0, 5, 2)),
           ("few-bad-2", (1, 0, 2, 2006, 5), (8, 0, 0, 4, 1))]
DEV_S1B = [("b1", (1, 0, 24, 2003, 5), (22, 0, 0, 4, 1)),
           ("b2", (0, 0, 15, 2008, 3), (12, 0, 0, 5, 0)),
           ("b3", (3, 0, 19, 2005, 7), (28, 0, 0, 3, 1)),
           ("b4", (0, 0, 31, 2001, 6), (30, 0, 0, 6, 0))]
DEV_PROBE_N = [5, 6, 8, 10]
DEV_PROBE_P95 = [38, 44, 41, 36]


# holdout: dev와 겹치지 않는 숫자
HOLD_S1A = [("few-ok-1", (4, 0, 0, 47, 3), (5, 0, 0, 4, 3)),
            ("few-ok-2", (1, 0, 0, 39, 5), (2, 0, 0, 6, 2)),
            ("few-ok-3", (5, 0, 0, 42, 2), (9, 0, 0, 3, 3)),
            ("few-bad-1", (0, 0, 2, 2002, 5), (3, 0, 0, 5, 3)),
            ("few-bad-2", (2, 0, 4, 2009, 4), (7, 0, 0, 4, 1)),
            ("few-bad-3", (0, 0, 5, 2004, 3), (1, 0, 0, 5, 4))]
HOLD_S1B = [("b1", (2, 0, 18, 2002, 4), (25, 0, 0, 5, 1)),
            ("b2", (0, 0, 22, 2007, 8), (19, 0, 0, 3, 0)),
            ("b3", (0, 0, 12, 2010, 3), (14, 0, 0, 6, 1)),
            ("b4", (5, 0, 35, 2004, 5), (30, 0, 0, 4, 0)),
            ("b5", (1, 0, 29, 2006, 6), (33, 0, 0, 5, 1)),
            ("b6", (2, 0, 16, 2001, 7), (11, 0, 0, 4, 1))]
HOLD_PROBE_N = [8, 5, 10, 6, 8, 5]
HOLD_PROBE_P95 = [37, 45, 40, 42, 39, 36]


def cases(s1a, s1b, probe_n, probe_p95):
    """-> [(state_name, label, expected, stage, obs_args, probe)]"""
    out = []
    for lab, d, l in s1a:
        out.append(("S1a", lab, "observe_more", "s1", (d, l), None))
    for lab, d, l in s1b:
        out.append(("S1b", lab, "probe_standby", "s1", (d, l), None))
    for i, (lab, d, l) in enumerate(s1b):
        out.append(("S2a", lab, "switch_to_standby", "s2", (d, l), (probe_n[i], probe_n[i], probe_p95[i])))
        out.append(("S2b", lab, "escalate", "s2", (d, l), (probe_n[i], 0, None)))
    return out


def call(state, instr, criteria):
    body = json.dumps({"state": state, "questions": {"next": {"type": "choice", "instructions": instr,
                                                              "criteria": criteria}}}).encode()
    req = urllib.request.Request(URL, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.loads(r.read())
    a = out["answers"]["next"]
    return a["choice"], a["probabilities"], out["timing_ms"]


def run(case_list, bases, wording):
    rows = []
    for name, lab, exp, stage, (d, l), probe in case_list:
        for b in bases:
            instr, crit = WORDINGS[wording][stage]
            ch, pr, ms = call(build(b, d, l, probe), instr, crit)
            rows.append(dict(w=wording, state=name, lab=lab, base=b, exp=exp, got=ch, probs=pr,
                             pexp=pr.get(exp, 0.0), pgot=pr.get(ch, 0.0), ms=ms))
    return rows


def report(rows, wording, per_call):
    R = [r for r in rows if r["w"] == wording]
    names = ["S1a", "S1b", "S2a", "S2b"]
    print(f"\n######## Wording {wording} ########")
    print(f"{'state':6}{'correct':>10}{'p>=0.7 (of correct)':>22}{'median p(expected)':>20}")
    for n in names:
        rs = [r for r in R if r["state"] == n]
        ok = [r for r in rs if r["got"] == r["exp"]]
        print(f"{n:6}{len(ok):>6}/{len(rs):<3}{sum(r['pgot'] >= 0.7 for r in ok):>22}"
              f"{st.median(r['pexp'] for r in rs):>20.2f}")
    tot = sum(r["got"] == r["exp"] for r in R)
    print(f"TOTAL correct {tot}/{len(R)}")
    print("Wrong answers by chosen option:")
    for n in names:
        c = Counter(r["got"] for r in R if r["state"] == n and r["got"] != r["exp"])
        print(f"  {n}: " + (", ".join(f"{k} {v}" for k, v in c.most_common()) or "none"))
    e1 = [r for r in R if r["state"] == "S1b"]
    e2 = [r for r in R if r["state"] == "S2b"]
    print(f"S1b answered observe_more: {sum(r['got'] == 'observe_more' for r in e1)}/{len(e1)}")
    print(f"S2b answered switch_to_standby: {sum(r['got'] == 'switch_to_standby' for r in e2)}/{len(e2)}")
    cw = [r for r in R if r["got"] != r["exp"] and r["pgot"] >= 0.7]
    print(f"Confidently wrong (chosen p>=0.7): {len(cw)}")
    for r in cw:
        print(f"  {r['state']} {r['lab']} {r['base']} exp={r['exp']} got={r['got']} p={r['pgot']:.2f}")
    print("Baseline flips (variants whose choice differed across the 3 baselines):")
    for n in names:
        by = defaultdict(list)
        for r in R:
            if r["state"] == n:
                by[r["lab"]].append(r)
        fl = {k: v for k, v in by.items() if len({x["got"] for x in v}) > 1}
        print(f"  {n}: {len(fl)}/{len(by)} variants flipped")
        for k, v in fl.items():
            print(f"     {k}: " + ", ".join(f"{x['base']}->{x['got']}" for x in v))
    pairs = both = 0
    a = {(r["lab"], r["base"]): r for r in R if r["state"] == "S2a"}
    b = {(r["lab"], r["base"]): r for r in R if r["state"] == "S2b"}
    for k in a:
        pairs += 1
        both += a[k]["got"] == a[k]["exp"] and b[k]["got"] == b[k]["exp"]
    print(f"Stage-2 pairs (healthy-probe AND failing-probe both correct): {both}/{pairs}")
    print(f"Median call time: {st.median(r['ms'] for r in R):.1f} ms")
    if per_call:
        print("Per call:")
        for r in R:
            pr = {k: round(p, 2) for k, p in r["probs"].items()}
            print(f"  {r['state']} {r['lab']} {r['base']} exp={r['exp']} got={r['got']} {pr}")


def dev_key(rows, w):
    R = [r for r in rows if r["w"] == w]
    ok = sum(r["got"] == r["exp"] for r in R)
    bad = sum((r["state"] == "S1b" and r["got"] == "observe_more") or
              (r["state"] == "S2b" and r["got"] == "switch_to_standby") for r in R)
    hi = sum(r["got"] == r["exp"] and r["pgot"] >= 0.7 for r in R)
    return ok, -bad, hi


def main():
    mode = sys.argv[1]
    if mode == "dev":
        cl = cases(DEV_S1A, DEV_S1B, DEV_PROBE_N, DEV_PROBE_P95)
        ws = sys.argv[2:] or [w for w in WORDINGS]
        rows = []
        for w in ws:
            rows += run(cl, DEV_BASES, w)
        for w in ws:
            report(rows, w, False)
        print("\nDEV SCORE (correct, -bad_errors, p>=0.7 correct):")
        for w in ws:
            print(f"  {w}: {dev_key(rows, w)}")
    else:
        w = sys.argv[2]
        cl = cases(HOLD_S1A, HOLD_S1B, HOLD_PROBE_N, HOLD_PROBE_P95)
        rows = run(cl, HOLD_BASES, w)
        report(rows, w, True)


if __name__ == "__main__":
    main()
