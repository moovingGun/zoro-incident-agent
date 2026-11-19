"""에이전트 기록(events)과 정상 클라이언트 요청 기록에서, 화면이 그리는 사건 한 건의 데이터를 만든다.

재생 화면(build_replay.py)과 실시간 화면(server.py)이 같은 형태를 쓴다.
시간은 모두 기준 시각(재생: 주입 시각, 실시간: 첫 이상 판정 시각)으로부터의 초.

판정 시각은 판정이 실제로 나온 때(decided_t)를 쓴다. 예전 기록에는 관측 시각(t)만 있으므로
그 관측으로 부른 모델 호출 시간을 더해 계산한다.
"""
PATH_NAMES = {"db": "DB 의존 경로", "light": "경량 경로"}
PAD = 6.0   # 사건 앞뒤로 보여 줄 여유(초)
FAULT = {"app_down": "앱 다운", "slow": "지연", "source_flood": "요청 폭주", "normal": "정상", "unknown": "판단 불가"}
VERDICT = {"recovered": "복구", "not_recovered": "미복구", "insufficient_data": "관측 부족"}
ACTION = {"restart_app": "앱 재시작", "limit_top_source": "출발지 제한", "switch_to_standby": "예비 앱 전환"}
STEP = {"observe_more": "새 요청 더 관측", "probe_standby": "예비 앱 점검", "switch_to_standby": "예비 앱으로 전환",
        "escalate": "담당자 이관"}


def _p95(ms):
    if not ms:
        return None
    s = sorted(ms)
    return round(s[min(len(s) - 1, int(0.95 * len(s)))], 1)


def _said(v):
    return VERDICT.get(v["label"], v["label"]) + (f" {v['p']:.2f}" if v["p"] is not None else " (규칙 대체)")


def _seen(e):
    """다음 단계 질문에 넣은 관측을 한 줄로 (점검 전: 주 앱 관측만, 점검 후: 거기에 예비 앱 점검 결과)."""
    a = e["observed"]["after_action"]
    pct = lambda x: "-" if x is None else f"{round(x * 100)}%"  # noqa: E731
    text = (f"주 앱: 재시작 뒤 끝난 요청 {a['completed_requests']}건 · DB 의존 경로 성공률 {pct(a['db_path']['success_rate'])}, "
            f"p95 {a['db_path']['p95_latency_ms']}ms · 경량 경로 성공률 {pct(a['light_path']['success_rate'])}")
    pr = e["observed"].get("standby_probe")
    if pr:
        text += (f" + 예비 앱 점검: {pr['requests_sent']}건 중 {pr['succeeded']}건 성공"
                 + (f", p95 {pr['p95_latency_ms']}ms" if pr["succeeded"] else ""))
    return text


def next_row(e, at):
    """다음 단계 선택 한 줄: 후보 -> Zoro-1 선택과 확률 -> 실행한 것, 또는 실제 점검 결과."""
    if e["type"] == "probe":
        r = e["result"]
        return {"at": at, "kind": "probe", "sent": r["requests_sent"], "ok": r["succeeded"], "p95": r["p95_latency_ms"]}
    v = e["verdict"]
    return {"at": at, "kind": "choice", "seen": _seen(e), "candidates": [STEP.get(c, c) for c in e["candidates"]],
            "choice": STEP.get(v["label"], v["label"]), "p": v.get("p"), "by": v["by"], "ms": v.get("ms"),
            "probs": [[STEP.get(k, k), round(x, 2)] for k, x in (v.get("probabilities") or {}).items()],
            "decision": e["decision"]}


def verdict_row(e, when, origin_t, mark=False):
    v = e["verdict"]
    check = e["type"] == "verification"
    return {"at": round(when(e) - origin_t, 3), "phase": e["type"], "label": v["label"], "p": v.get("p"),
            "by": v["by"], "rule": v["rule"], "ms": v.get("ms"), "conflict": bool(e.get("conflict")), "mark": mark,
            "completed": e["after"]["completed_requests"] if check else None,
            "in_flight": e["after"]["in_flight_requests"] if check else None}


def decided_clock(events):
    """기록 한 줄 -> 그 판정이 실제로 나온 시각."""
    call_ms = {e["t"]: e["verdict"].get("ms") or 0 for e in events if e["type"] in ("diagnosis", "verification")}

    def when(e):
        if "decided_t" in e:
            return e["decided_t"]
        if e["type"] in ("action_done", "acknowledged", "closed"):   # 이 줄들의 t는 이미 그 일이 끝난 시각
            return e["t"]
        return e["t"] + call_ms.get(e["t"], 0) / 1000
    return when


def build(events, requests, incident_id, origin_t=None, origin_label="첫 이상 판정", now=None):
    """origin_t가 없으면 첫 이상 판정 시각을 기준으로 삼는다 (실시간: 에이전트는 주입 시각을 모른다).
    requests: [{path, start, t(끝난 시각, 진행 중이면 None), result, ms}]."""
    when = decided_clock(events)
    mine = [e for e in events if e.get("incident") == incident_id]
    opened = next(e for e in mine if e["type"] == "incident")
    ended = next((e for e in mine if e["type"] in ("resolved", "escalated")), None)
    actions_done = [e for e in mine if e["type"] == "action_done"]
    action_start = next((e for e in mine if e["type"] == "action_start"), None)
    ack = next((e for e in mine if e["type"] == "acknowledged"), None)
    # 감시 재개: 측정값이 정상으로 돌아와 새 사건 감시를 다시 시작한 때 (예전 기록에서는 closed)
    resumed = next((e for e in mine if e["type"] in ("monitoring_resumed", "closed")), None)
    alerts = [e for e in mine if e["type"] == "alert"]   # 이 사건이 계속되는 중에 감지된 다른 유형의 장애
    baseline = next((e["baseline"] for e in reversed(events) if e["type"] == "baseline" and e["t"] <= opened["t"]), None)

    # 첫 이상 판정: 이 사건과 같은 유형이 기준 확률을 처음 넘은 때 (그 뒤 끊겼다가 다시 넘었을 수 있다). 기록이 없으면 비운다
    earliest = max(opened["t"] - 15, origin_t - 1) if origin_t is not None else opened["t"] - 15
    first = next((e for e in events if e["type"] == "suspect" and e.get("fault") == opened["fault"]
                  and earliest <= e["t"] <= opened["t"]), None)
    if origin_t is None:
        origin_t = when(first or opened)
    t_end = when(ended) if ended else (now or events[-1]["t"])
    later = [e["t"] for e in alerts + ([resumed] if resumed else []) + ([ack] if ack else [])]
    lo, hi = min(origin_t, opened["t"]) - PAD, min(max([t_end] + later), t_end + 90) + PAD   # 이관 뒤 일도 90초까지는 그림에
    if ended and ended["type"] == "escalated" and not resumed and now:
        # 이관한 장애가 아직 계속되는 중: 그림이 이관 시점에 멈춰 있지 않고 지금까지 이어지게 한다 (최근 3분까지)
        hi = now + PAD
        lo = max(lo, hi - 180)
    rel = lambda t: round(t - origin_t, 3)  # noqa: E731

    marks = {e["t"] for e in (first, opened, ended) if e}
    verdicts = [verdict_row(e, when, origin_t, e["t"] in marks) for e in events
                if lo <= e["t"] <= hi and (e["type"] == "diagnosis" or
                                           (e["type"] == "verification" and e.get("incident") == incident_id))]
    checks = [e for e in mine if e["type"] == "verification"]
    opening = next((e for e in events if e["type"] == "diagnosis" and e["t"] == opened["t"]), None)
    last = checks[-1]["verdict"] if checks else None

    kind = None
    if ended and ended["type"] == "escalated":   # 실제 이관 사유. 예전 기록에는 kind가 없어 사유 문구로 가린다
        kind = ended.get("kind") or ("no_action" if not actions_done else
                                     "undetermined" if "판정하지 못함" in ended.get("reason", "") else "not_recovered")

    milestones = []
    if first:
        milestones.append({"key": "suspect", "label": "첫 이상 판정", "at": rel(when(first)),
                           "detail": f"기준 확률 이상으로 처음 판정: {FAULT.get(opened['fault'], opened['fault'])}"})
    said = (FAULT.get(opening["verdict"]["label"], opening["verdict"]["label"]) +
            (f" {opening['verdict']['p']:.2f}" if opening["verdict"]["p"] is not None else " (규칙 대체)")) if opening else \
        FAULT.get(opened["fault"], opened["fault"])
    milestones.append({"key": "incident", "label": "사건 시작", "at": rel(when(opened)),
                       "detail": f"새 요청이 쌓인 뒤 같은 판정: {said}"})
    for a in actions_done:
        ok = a.get("ok", True)
        milestones.append({"key": "action_done", "at": rel(when(a)),
                           "label": {"restart_app": "재시작 완료", "switch_to_standby": "전환 완료"}.get(a["action"], "조치 완료")
                           if ok else "조치 실패",
                           "detail": f"{ACTION.get(a['action'], a['action'])}: {a['result']}"})
    probe = next((e for e in mine if e["type"] == "probe"), None)   # 첫 점검만 단계 줄에 올린다 (전부는 next_steps에)
    if probe:
        picked = next((e["verdict"] for e in mine if e["type"] == "next_step" and e["decision"] == "probe_standby"), None)
        r = probe["result"]
        milestones.append({"key": "probe", "label": "예비 앱 점검", "at": rel(when(probe)),
                           "detail": (f"Zoro-1 선택 {picked['p']:.2f} → " if picked and picked.get("p") is not None else "")
                           + f"주문 조회 {r['requests_sent']}건 중 {r['succeeded']}건 성공"})
    if ended:
        detail = (f"판정 불일치: 모델 {_said(last)}, 측정값 {VERDICT.get(last['rule'], last['rule'])}" if kind == "disagreement" and last
                  else ended.get("reason", "") if kind in ("undetermined", "no_action", "action_failed") or not last
                  else f"조치 후 판정: {_said(last)}")
        milestones.append({"key": ended["type"], "at": rel(when(ended)), "detail": detail,
                           "label": "복구 확인" if ended["type"] == "resolved" else "당직자 이관"})
    for a in alerts:
        milestones.append({"key": "alert", "label": "다른 장애 경보", "at": rel(when(a)),
                           "detail": f"{FAULT.get(a['fault'], a['fault'])} 감지. 자동 조치 없음"})
    if resumed:
        milestones.append({"key": "resumed", "label": "감시 재개", "at": rel(when(resumed)),
                           "detail": "측정값 정상화. 사건은 사람에게 남아 있음"})
    if ack:
        milestones.append({"key": "ack", "label": "담당자 인수", "at": rel(ack["t"]), "detail": "담당자가 화면에서 인수함"})
    milestones.sort(key=lambda m: m["at"])

    # 경로별 측정값. 조치 전 = 사건을 연 5초 창. 조치 후 = 에이전트가 마지막 검증에 쓴 집계 그대로
    # (예전 기록에는 그 집계가 없어, 조치가 끝난 뒤 시작한 요청으로 다시 센다)
    counts = checks[-1].get("counts") if checks else None
    act_t = actions_done[-1]["t"] if actions_done else None
    paths = []
    for p, name in PATH_NAMES.items():
        row = {"key": p, "name": name, "before": None, "after": None}
        if opening:
            b = opening["window"]["normal_user"][p + "_path"]
            row["before"] = {"success": b["success"], "failed": b["failed"], "timeout": b["timeout"],
                             "in_flight": b["in_flight"], "p95": b["p95_latency_ms"]}
        if counts:
            c = counts[p + "_path"]
            row["after"] = {"success": c["success"], "failed": c["failed"], "timeout": c["timeout"],
                            "in_flight": c["in_flight"], "p95": c["p95_latency_ms"]}
        elif act_t is not None and checks:
            cut = checks[-1]["t"]
            started = [r for r in requests if r["path"] == p and act_t <= r["start"] <= cut]
            done = [r for r in started if r["t"] is not None and r["t"] <= cut]
            row["after"] = {"success": sum(r["result"] == "success" for r in done),
                            "failed": sum(r["result"] == "failed" for r in done),
                            "timeout": sum(r["result"] == "timeout" for r in done),
                            "in_flight": len(started) - len(done), "p95": _p95([r["ms"] for r in done])}
        paths.append(row)

    first_action = actions_done[0] if actions_done else None
    return {
        "id": incident_id, "fault": opened["fault"], "origin_label": origin_label, "origin_t": origin_t,
        # 판정 시각의 근거: 에이전트가 직접 남긴 값인지, 관측 시각에 모델 호출 시간을 더해 추정한 값인지
        "time_basis": "recorded" if "decided_t" in opened else "estimated",
        "baseline": baseline, "outcome": ended["type"] if ended else "open", "reason": (ended or {}).get("reason"),
        "kind": kind, "range": [rel(lo), rel(hi)], "end": rel(t_end),
        "ack": {"at": rel(ack["t"])} if ack else None, "resumed": {"at": rel(when(resumed))} if resumed else None,
        "alerts": [{"at": rel(when(a)), "fault": a["fault"]} for a in alerts],
        "milestones": milestones,
        "action": {"name": action_start["action"], "start": rel(when(action_start)),
                   "done": rel(when(first_action)) if first_action else None,
                   "result": first_action["result"] if first_action else None,
                   "ok": first_action.get("ok", True) if first_action else None,
                   "counted_from": rel(act_t) if act_t is not None else None} if action_start else None,
        "verdicts": verdicts, "paths": paths,
        "last_action": actions_done[-1]["action"] if actions_done else None,
        "next_steps": [next_row(e, rel(when(e))) for e in mine if e["type"] in ("next_step", "probe")],
        "requests": [{"p": r["path"], "s": rel(r["start"]), "e": rel(r["t"]) if r["t"] is not None else None,
                      "r": r.get("result"), "ms": r.get("ms")} for r in requests if lo <= r["start"] <= hi],
    }
