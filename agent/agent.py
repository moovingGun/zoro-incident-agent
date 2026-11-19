"""장애 대응 에이전트.

역할 분담
  Zoro-1   : 진단(정상/앱 다운/요청 폭주/지연/판단 불가), 조치 후 검증(복구/미복구/관측 부족)
  코드 정책 : 어떤 조치를 어떤 순서로 몇 번까지 할지, 조치가 지금도 유효한지, 언제 이관할지
모델 호출이 실패하면 규칙 판정으로 이어 간다. 규칙 판정은 평소에도 같이 계산해 기록한다.
사건은 모델이 이상이라고 판정하고, 측정값에도 사용자 영향이 있을 때(규칙 판정이 정상이 아닐 때)만 연다.
조치 후 검증도 모델 판정이 측정값과 같을 때만 받아들인다. 다르면 보류하고, 제한 시간까지 그대로면
'미복구'가 아니라 '판정 불일치'로 이관한다.

    .venv/bin/python agent/zoro_server.py     # 먼저 띄운다
    .venv/bin/python agent/agent.py           # 서비스가 정상일 때 시작 (처음 10초 동안 평소 기준을 잰다)

기록: logs/agent/events.jsonl (판정·조치 전부), logs/agent/reports/ (이관 보고서)
"""
import json
import os
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from observe import PATHS, ROOT, Observer, p95  # noqa: E402

ZORO_URL = "http://127.0.0.1:8765/decide"
PROBE_URL = "http://127.0.0.1:8080/api/ping"
SPAN = 5.0            # 진단에 쓰는 창 길이(초)
TICK = 1.0            # 판정 주기(초)
CONF = 0.7            # 판정을 받아들이는 최소 확률. 실행 조건일 뿐 정확성 보증은 아니다
MIN_NEW = 5           # 첫 판정 이후 새로 끝난 요청이 이만큼 쌓여야 같은 판정을 "두 번째"로 센다 (겹치는 창 대비)
MIN_SAMPLES = 20      # 복구 검증에 필요한, 조치 후 시작해서 끝난 요청 수
VERIFY_TIMEOUT = 30   # 조치 후 이 시간(초) 안에 판정이 안 나면 이관
MAX_ACTIONS = 2       # 사건당 자동 조치 상한
POLICY = {            # 진단 결과 -> 허용된 자동 조치 순서. DB 재시작은 어디에도 없다
    "app_down": ["restart_app"],
    "source_flood": ["limit_top_source"],
    "slow": ["restart_app"],
}

DIAG_Q = {"fault": {
    "type": "choice",
    "instructions": "Compare this 5-second window with the normal baseline. What is the state of the service?",
    "criteria": {
        "normal": "normal users' requests succeed and latency is near the baseline on both paths",
        "app_down": "nginx cannot reach the app: connection errors to the app and 5xx responses on both paths",
        "source_flood": "request volume is far above the baseline and one source sends most of it",
        # 실제 창을 다시 물어 비교한 뒤 고른 문구 (agent/replay.py)
        "slow": "the app is reachable, but at least one path is timing out or much slower than the baseline",
        "unknown": "abnormal, but none of the patterns above",
    }}}

VERIFY_Q = {"verdict": {
    "type": "choice",
    "instructions": "The action below was executed and its command succeeded. Judging only by requests from normal "
                    "users that started after the action, did the service recover?",
    "criteria": {
        "recovered": "enough samples, requests succeed, and latency is back near the normal baseline on every path",
        "not_recovered": "enough samples, but some path is still failing or still much slower than the normal baseline",
        "insufficient_data": "too few completed requests after the action to tell yet",
    }}}

# 재시작 후에도 미복구일 때 다음 단계를 고르는 질문. 후보(criteria의 키)는 코드가 정하고 Zoro-1은 그중에서 고른다.
# 점검 전(1)에는 주 앱 관측만, 점검 후(2)에는 거기에 예비 앱 점검 결과만 더해 준다.
# 문구는 사전 테스트에서 고른 것을 그대로 쓴다 (zoro_nextstep_test.py의 C).
STANDBY_URL = "http://127.0.0.1:8081/api/orders"   # 예비 앱에 직접 보내는 점검 요청 (사용자 트래픽은 nginx를 거친다)
PROBE_N = 5           # 점검 한 번에 보내는 주문 조회 수
NEXT_TIMEOUT = 30     # 다음 단계를 이 시간(초) 안에 정하지 못하면 이관
_INTRO = "An incident was detected and the main app was restarted; the restart command succeeded. "
_CTX = ("db_path is the orders API and reads the database; light_path is a ping API that does not use the database. "
        "The client timeout is 2 seconds, so a path with p95 around 2000 ms is timing out. ")
NEXT_Q = {
    1: {"next": {
        "type": "choice",
        "instructions": _INTRO + "The state shows what happened to requests after the restart. " + _CTX +
                        "A standby app exists that uses the same database as the main app. First check enough_samples: "
                        "if it is false, no conclusion can be drawn yet, whatever the finished requests show. "
                        "Choose the single best next step.",
        "criteria": {
            "observe_more": "enough_samples is false: too few requests have finished to judge anything, even if some "
                            "of the finished requests failed or timed out; wait for more requests to finish before "
                            "deciding.",
            "probe_standby": "enough_samples is true and db_path is still failing while light_path is fine; send the "
                             "same db_path request to the standby app to learn whether the problem is only in the "
                             "main app.",
            "escalate": "Hand the incident over to the on-call person.",
        }}},
    2: {"next": {
        "type": "choice",
        "instructions": _INTRO + "The main app is still failing on db_path. The same db_path request was then sent to "
                                 "the standby app, which uses the same database. " + _CTX +
                                 "Choose the single best next step.",
        "criteria": {
            "observe_more": "The standby probe result is not conclusive; wait for more results before deciding.",
            "switch_to_standby": "The standby app answers db_path requests successfully while the main app does not; "
                                 "route user traffic to the standby app.",
            "escalate": "The standby app fails db_path requests too, so switching would not help; hand the incident "
                        "over to the on-call person.",
        }}},
}
STEP = {"observe_more": "새 요청 더 관측", "probe_standby": "예비 앱 점검", "switch_to_standby": "예비 앱으로 전환",
        "escalate": "담당자 이관"}

LOG_DIR = os.path.join(ROOT, "logs/agent")
os.makedirs(os.path.join(LOG_DIR, "reports"), exist_ok=True)
events = open(os.path.join(LOG_DIR, "events.jsonl"), "a", buffering=1)
obs = None


def clock(t):
    return time.strftime("%H:%M:%S", time.localtime(t))


def emit(event, text, incident=None, **data):
    # t = 판단에 쓴 마지막 관측의 시각 (기록에 찍힌 컨테이너 시계). 조치 후 집계의 경계로 쓴다.
    # decided_t = 이 줄을 실제로 남긴 호스트 시각 (모델 호출과 조치가 끝난 뒤). 주입 시각과 같은 시계라 경과 시간을 잴 때 쓴다.
    decided = round(time.time(), 3)
    events.write(json.dumps({"t": round(obs.now, 3), "decided_t": decided, "type": event,
                             "incident": incident and incident["id"], **data},
                            ensure_ascii=False) + "\n")
    print(f"{clock(obs.now)} {text}", flush=True)


# ---- 판정: Zoro-1, 실패하면 규칙 -------------------------------------------------------------------

def path_bad(s, base_p95):
    """규칙: 이 경로가 사용자에게 나쁜 상태인가 (성공률 90% 미만이거나 평소보다 훨씬 느림)."""
    n = s["success"] + s["failed"] + s["timeout"]
    return bool(n) and (s["success"] / n < 0.9 or s["p95_latency_ms"] > max(3 * base_p95, base_p95 + 50))


def rule_diagnose(w, base):
    c = w["client_to_nginx"]
    if w["nginx_to_app"]["connection_error_share"] >= 0.3 or c["status_5xx_share"] >= 0.3:
        return "app_down"
    if c["requests_per_s"] >= 3 * base["requests_per_s"] and c["top_source_share"] >= 0.5:
        return "source_flood"
    if any(path_bad(w["normal_user"][p + "_path"], base[p + "_path_p95_ms"]) for p in PATHS):
        return "slow"
    return "normal"


def enough_samples(after):
    """복구 여부를 말할 수 있는 표본인가: 전체 MIN_SAMPLES건 이상이고, 경로마다 MIN_NEW건 이상 끝났다.
    한 경로가 응답 대기뿐이면(끝난 요청이 없으면) 그 경로의 결과를 모르는 것이므로 관측 부족이다."""
    per_path = [sum(after[p + "_path"][k] for k in ("success", "failed", "timeout")) for p in PATHS]
    return after["completed_requests"] >= MIN_SAMPLES and min(per_path) >= MIN_NEW


def rule_verify(after, base):
    if not enough_samples(after):
        return "insufficient_data"
    bad = any(path_bad(after[p + "_path"], base[p + "_path_p95_ms"]) for p in PATHS)
    return "not_recovered" if bad else "recovered"


def judge(state, questions, rule_label):
    """-> {"label", "p", "by": "zoro" | "rule_fallback", "rule": 규칙이 낸 답, ...}"""
    qid = next(iter(questions))
    try:
        body = json.dumps({"state": state, "questions": questions}).encode()
        req = urllib.request.Request(ZORO_URL, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            out = json.loads(r.read())
        a = out["answers"][qid]
        return {"label": a["choice"], "p": a["probabilities"][a["choice"]], "probabilities": a["probabilities"],
                "by": "zoro", "ms": out["timing_ms"], "rule": rule_label}
    except Exception as e:   # noqa: BLE001 -- 모델을 못 쓰면 규칙으로 이어 간다
        return {"label": rule_label, "p": None, "by": "rule_fallback", "error": f"{type(e).__name__}: {e}",
                "rule": rule_label}


def accepted(v):
    return v["by"] == "rule_fallback" or v["p"] >= CONF


def show(v):
    if v["by"] == "zoro":
        return f"{v['label']} {v['p']:.2f} (Zoro-1 {v['ms']:.0f}ms, 규칙: {v['rule']})"
    return f"{v['label']} (규칙 대체, 모델 호출 실패)"


# ---- 조치 실행 -------------------------------------------------------------------------------------

def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True, text=True, timeout=60)


def restart_app(_inc):
    compose("restart", "app")
    for _ in range(50):   # 재시작 명령이 끝나도 앱이 응답하기까지 잠깐 걸린다. 응답할 때까지를 조치로 본다
        try:
            if urllib.request.urlopen(PROBE_URL, timeout=1).status == 200:
                return "앱이 다시 응답함"
        except Exception:   # noqa: BLE001
            pass
        time.sleep(0.2)
    return "재시작 명령은 성공했지만 10초 안에 응답이 없음"


def limit_top_source(inc):
    with open(os.path.join(ROOT, "services/nginx/dynamic/blocked.conf"), "w") as f:
        f.write(f"deny {inc['top_source']};\n")
    compose("exec", "-T", "nginx", "nginx", "-s", "reload")
    return f"{inc['top_source']} 차단"


class NotReady(Exception):
    """조치 명령은 성공했지만 준비 확인이 끝까지 실패했다. 조치 성공으로 기록하지 않는다."""


def route(conf):
    with open(os.path.join(ROOT, "services/nginx/dynamic/upstream.conf"), "w") as f:
        f.write(conf)
    compose("exec", "-T", "nginx", "nginx", "-s", "reload")


def switch_to_standby(_inc):
    # 전환된 설정으로 처리한 응답에는 X-Routed-To: standby가 붙는다. 이 설정에서 요청은 예비 앱으로만 간다.
    route("set $upstream http://standby:8000;\nadd_header X-Routed-To standby always;\n")
    # reload 명령은 바로 돌아오지만 실제 적용까지 잠깐 걸린다. 그 사이에 시작한 요청은 아직 주 앱으로 간다.
    # 전환된 설정을 거친 주문 조회가 200으로 응답할 때까지를 조치로 본다 (그 뒤에 시작한 요청만 검증에 쓰인다).
    # 주 앱이 낸 200(표시 없음)이나 예비 앱 오류(502)는 준비 확인으로 치지 않는다.
    for _ in range(20):
        try:
            r = urllib.request.urlopen(PROBE_URL.replace("ping", "orders"), timeout=0.5)
            if r.status == 200 and r.headers.get("X-Routed-To") == "standby":
                return "전환 명령 성공, 예비 앱 경로로 보낸 주문 조회 응답 확인"
        except Exception:   # noqa: BLE001
            pass
        time.sleep(0.1)
    route("")   # 예비 앱이 응답하는지 확인하지 못한 채 사용자를 그쪽에 두지 않는다
    raise NotReady("전환 명령(nginx reload)은 성공했지만 예비 앱 경로의 주문 조회가 응답하지 않아 준비 확인 실패. 전환을 되돌림")


ACTIONS = {"restart_app": restart_app, "limit_top_source": limit_top_source, "switch_to_standby": switch_to_standby}


def probe_standby():
    """예비 앱에 주문 조회를 직접 보낸다. 사용자 트래픽과 별개인 점검 요청이고, 시간 제한은 클라이언트와 같은 2초.
    timed_out에는 2초 안에 정상 응답을 받지 못한 요청을 모두 센다."""
    def one(_):
        t0 = time.time()
        try:
            ok = urllib.request.urlopen(STANDBY_URL, timeout=2).status == 200
        except Exception:   # noqa: BLE001
            ok = False
        return ok, round((time.time() - t0) * 1000, 1)
    with ThreadPoolExecutor(PROBE_N) as pool:
        got = list(pool.map(one, range(PROBE_N)))
    fine = [ms for ok, ms in got if ok]
    return {"path": "db_path", "requests_sent": PROBE_N, "succeeded": len(fine), "timed_out": PROBE_N - len(fine),
            "p95_latency_ms": p95(fine) if fine else None}


def probe_ok(probe, base):
    """코드 쪽 전환 조건: 점검 요청이 90% 이상 성공하고 평소보다 훨씬 느리지 않다 (path_bad와 같은 기준)."""
    s = {"success": probe["succeeded"], "failed": 0, "timeout": probe["timed_out"],
         "p95_latency_ms": probe["p95_latency_ms"] or 0}
    return probe["succeeded"] > 0 and not path_bad(s, base["db_path_p95_ms"])


def rule_next(state, base):
    """다음 단계의 규칙 판정 (모델을 못 쓸 때 대신 쓰고, 평소에도 같이 기록한다)."""
    probe = state.get("standby_probe")
    if probe is None:
        return "probe_standby" if state["after_action"]["enough_samples"] else "observe_more"
    return "switch_to_standby" if probe_ok(probe, base) else "escalate"


def still_valid(action, w, base):
    """실행 직전 확인: 이 조치가 지금 관측에서도 말이 되는가."""
    if action == "limit_top_source":
        c = w["client_to_nginx"]
        return c["top_source_share"] >= 0.5 and c["requests_per_s"] >= 2 * base["requests_per_s"]
    return True


# ---- 사건 처리 -------------------------------------------------------------------------------------

def verify_request_state(action, base, after):
    """검증 질문에 넣는 상태. after = 조치가 끝난 뒤 시작한 요청의 집계 (Observer.since)."""
    state_after = {"completed_requests": after["completed_requests"], "in_flight_requests": after["in_flight_requests"],
                   "enough_samples": enough_samples(after)}
    for p in PATHS:
        s = after[p + "_path"]
        n = s["success"] + s["failed"] + s["timeout"]
        state_after[p + "_path"] = {"success_rate": round(s["success"] / n, 2) if n else None,
                                    "p95_latency_ms": s["p95_latency_ms"], "timeouts": s["timeout"],
                                    "in_flight": s["in_flight"]}
    return {"action": action, "normal_baseline": {p + "_path_p95_ms": base[p + "_path_p95_ms"] for p in PATHS},
            "min_samples_required": MIN_SAMPLES, "after_action": state_after}


# 검증 질문에 넣는 조치 이름. 같은 관측인데도 "switch_to_standby"라고 적으면 복구 확률이 낮게 나왔다
# (기록된 전환 후 관측 22건: 0.7 이상 7건 -> 이 이름으로는 14건, restart_app과 같은 수준). 질문 문구와 기준 확률은 그대로다.
VERIFY_NAME = {"switch_to_standby": "failover_to_standby_app"}


def verify_state(inc, base):
    after = obs.since(inc["action_t"])
    action = inc["tried"][-1]
    return verify_request_state(VERIFY_NAME.get(action, action), base, after), after


def check_verdict(inc, v, enough, new, now):
    """검증 판정 한 번을 반영한다. 결론이 났으면 "recovered" / "not_recovered", 아직이면 None.

    모델 판정은 측정값(규칙 판정)과 같을 때만 받아들인다. 다르면 보류하고 연속 판정을 처음부터 다시 센다.
    결론은 같은 판정이 두 번 나오고, 그 사이에 새로 끝난 요청이 MIN_NEW건 이상일 때 난다.
    """
    decided = v["label"] in ("recovered", "not_recovered") and accepted(v) and enough
    inc["conflict"] = decided and v["rule"] != v["label"]
    inc["checks"] += 1
    inc["conflicts"] += inc["conflict"]
    if not decided or inc["conflict"]:
        inc["streak"] = None
    elif inc["streak"] != v["label"]:
        inc["streak"], inc["last_check_t"] = v["label"], now     # 첫 번째 판정
    elif new >= MIN_NEW:                                         # 새 요청이 쌓인 뒤 같은 판정
        return v["label"]
    return None


def next_action(inc, w, base):
    """정책: 남은 허용 조치를 고른다. 없으면 (None, 이유)."""
    if len(inc["tried"]) >= MAX_ACTIONS:
        return None, f"사건당 자동 조치 상한({MAX_ACTIONS}회)에 도달"
    left = [a for a in POLICY[inc["fault"]] if a not in inc["tried"]]
    if not left:
        return None, "남은 허용 조치 없음" if inc["tried"] else "이 진단에 허용된 자동 조치 없음"
    if not still_valid(left[0], w, base):
        return None, f"{left[0]}: 현재 관측에서는 실행 조건을 충족하지 않음"
    return left[0], None


def run_action(inc, action):
    emit("action_start", f"조치 실행: {action}", inc, action=action)
    command_ok = True
    try:
        result, ok = ACTIONS[action](inc), True
    except NotReady as e:    # 명령은 성공, 준비 확인 실패
        result, ok = str(e), False
    except Exception as e:   # noqa: BLE001 -- 조치 명령 자체가 실패
        result, ok, command_ok = f"명령 실패: {type(e).__name__}: {e}", False, False
    obs.poll()
    inc["tried"].append(action)
    inc.update(action_t=obs.now, verify_wall=time.time(), streak=None, last_check_t=obs.now,
               checks=0, conflicts=0, conflict=False)
    inc["actions"].append({"action": action, "done_t": obs.now, "result": result, "ok": ok})
    emit("action_done", f"조치 {'완료' if ok else '실패'}: {action} -> {result}", inc, action=action, result=result, ok=ok,
         command_ok=command_ok)
    return ok   # False면 명령 자체가 실패했다. 검증 질문은 "명령이 성공했다"를 전제하므로 묻지 않고 이관한다


def run_probe(inc):
    probe = probe_standby()
    obs.poll()
    text = (f"예비 앱 점검: 주문 조회 {probe['requests_sent']}건 중 {probe['succeeded']}건 성공"
            + (f", p95 {probe['p95_latency_ms']}ms" if probe["succeeded"] else ""))
    inc["trail"].append(f"{clock(obs.now)} {text}")
    emit("probe", text, inc, target=STANDBY_URL, result=probe)
    return probe


def next_step(inc, base):
    """재시작 후에도 미복구인 사건에서 다음 단계를 Zoro-1이 고른다. 돌려주는 값은 다음 mode.

    선택은 기준 확률(CONF) 이상일 때만 따른다. 못 미치거나 '더 관측'이면 새 요청이 MIN_NEW건 쌓인 뒤에만
    다시 묻고, 점검 후라면 그때 점검도 다시 한다. 전환은 점검 결과가 코드 기준(probe_ok)으로도 충분할 때만 실행한다.
    """
    late = time.time() - inc["next_wall"] > NEXT_TIMEOUT
    if inc["asked_t"] is not None and not late:
        if obs.ended_since(inc["asked_t"], started_after=inc["action_t"]) < MIN_NEW:
            return "next"                       # 새 관측이 아직 없다
        if inc["probe"]:
            inc["probe"] = run_probe(inc)       # 점검 후 보류됐었다: 새 점검 결과가 새 관측이다
    if late:
        escalate(inc, base, "not_recovered", f"재시작 후에도 미복구. 다음 단계를 {NEXT_TIMEOUT}초 안에 정하지 못함")
        return "escalated"
    stage = 2 if inc["probe"] else 1
    after = obs.since(inc["action_t"])
    state = verify_request_state("restart_app (command succeeded)", base, after)
    if inc["probe"]:
        state["standby_probe"] = inc["probe"]
    v = judge(state, NEXT_Q[stage], rule_next(state, base))
    inc.update(after=after, asked_t=obs.now)
    choice, note = (v["label"], "") if accepted(v) else (None, " | 보류: 기준 확률 미만, 새 관측 뒤 다시 판정")
    if choice == "switch_to_standby" and not probe_ok(inc["probe"], base):
        choice, note = None, " | 보류: 모델은 전환을 골랐지만 점검 결과가 전환 조건에 못 미침"
    decision = {None: "hold", "observe_more": "hold"}.get(choice, choice)
    candidates = list(NEXT_Q[stage]["next"]["criteria"])
    said = (f"{STEP[v['label']]} {v['p']:.2f} (Zoro-1 {v['ms']:.0f}ms, 규칙: {STEP[v['rule']]})" if v["by"] == "zoro"
            else f"{STEP[v['label']]} (규칙 대체, 모델 호출 실패)")
    text = f"다음 단계 선택 [{' / '.join(STEP[c] for c in candidates)}] -> {said}{note}"
    inc["trail"].append(f"{clock(obs.now)} {text}")
    emit("next_step", text, inc, stage=stage, observed=state, candidates=candidates, verdict=v, decision=decision)
    if decision == "probe_standby":
        inc["probe"] = run_probe(inc)
        inc.update(asked_t=None, next_wall=time.time())   # 점검 결과가 새 관측이다: 바로 다음 판정으로
    elif decision == "switch_to_standby":
        if run_action(inc, "switch_to_standby"):
            return "verify"                     # 복구 여부는 정상 클라이언트가 nginx로 보낸 요청으로 확인한다
        escalate(inc, base, "action_failed", inc["actions"][-1]["result"])
        return "escalated"
    elif decision == "escalate":
        p = inc["probe"]
        escalate(inc, base, "not_recovered", "재시작 후에도 미복구. " + (
            f"예비 앱 점검도 실패(주문 조회 {p['requests_sent']}건 중 {p['succeeded']}건 성공)해 전환하지 않음" if p
            else "Zoro-1이 담당자 이관을 선택"))
        return "escalated"
    return "next"


KINDS = {"not_recovered": "미복구", "disagreement": "판정 불일치", "undetermined": "판정 미확정",
         "no_action": "자동 조치 없음", "action_failed": "조치 명령 실패"}


def escalate(inc, base, kind, why):
    """당직자에게 넘긴다. kind가 실제 사유다 (미복구와 판정 불일치를 섞지 않는다)."""
    report = os.path.relpath(write_report(inc, base, kind, why), ROOT)
    inc.update(escalated_t=obs.now, alerted=[])
    emit("escalated", f"{KINDS[kind]}: {why} -> 당직자 이관 ({report})", inc, kind=kind, reason=why,
         actions=inc["tried"], report=report, checks=inc.get("checks", 0), conflicts=inc.get("conflicts", 0))


def write_report(inc, base, kind, why):
    path = os.path.join(LOG_DIR, "reports", f"incident-{inc['id']}.md")
    after = inc.get("after")
    lines = [f"# 장애 이관 보고서 {inc['id']}", "",
             f"- 감지: {clock(inc['detected_t'])}, 진단 `{inc['fault']}` ({show(inc['diagnosis'])})",
             f"- 이관: {clock(obs.now)}, 사유: **{KINDS[kind]}** ({why})", "", "## 시도한 자동 조치", ""]
    lines += [f"- {clock(a['done_t'])} `{a['action']}`: {a['result']}" for a in inc["actions"]] or ["- 없음"]
    if inc.get("last_verdict"):
        lines += ["", f"조치 후 마지막 판정: {show(inc['last_verdict'])}",
                  f"검증 {inc['checks']}회 중 모델 판정과 측정값이 어긋나 보류한 횟수: {inc['conflicts']}회"]
    if inc.get("trail"):
        lines += ["", "## 다음 단계 선택 (후보는 코드가 정하고 Zoro-1이 고름)", ""] + [f"- {x}" for x in inc["trail"]]
    lines += ["", "## 정상 사용자 관측", "",
              "조치 후 값은 마지막 조치가 끝난 뒤 시작한 요청만 집계했습니다.", "",
              "| 경로 | 평소 p95 | 감지 당시 (5초 창) | 조치 후 |", "|---|---|---|---|"]
    cell = lambda s: (f"성공 {s['success']}, 실패 {s['failed']}, 타임아웃 {s['timeout']}, 진행 중 {s['in_flight']}, "  # noqa: E731
                      f"p95 {s['p95_latency_ms']}ms")
    names = {"db": "DB 의존 경로", "light": "경량 경로"}
    for p in PATHS:
        k = p + "_path"
        lines.append(f"| {names[p]} | {base[k + '_p95_ms']}ms | {cell(inc['before']['normal_user'][k])} | "
                     f"{cell(after[k]) if after else '-'} |")
    if after:
        bad = [names[p] for p in PATHS if path_bad(after[p + "_path"], base[p + "_path_p95_ms"])]
        good = [names[p] for p in PATHS if names[p] not in bad]
        did = ", ".join(a["action"] for a in inc["actions"])
        measured = (f"`{did}` 이후에도 {', '.join(bad)}의 문제가 지속됐습니다."
                    + (f" {', '.join(good)}는 정상 범위입니다." if good else "")) if bad else \
            f"`{did}` 이후 측정값은 두 경로 모두 정상 범위입니다."
        lines += ["", "## 관측으로 말할 수 있는 범위", "", measured]
        if kind == "disagreement":
            v = inc["last_verdict"]
            lines += [f"반면 모델의 마지막 판정은 `{v['label']}` {v['p']:.2f}였습니다. 제한 시간 {VERIFY_TIMEOUT}초 동안 "
                      "모델 판정과 측정값이 일치하지 않아 복구로도 미복구로도 확정하지 않았습니다. "
                      "어느 쪽이 맞는지는 사람이 확인해야 합니다."]
        elif kind == "undetermined":
            lines += [f"제한 시간 {VERIFY_TIMEOUT}초 안에 복구 여부를 확정할 판정이 나오지 않았습니다."]
        lines += ["원인은 이 관측만으로 단정하지 않습니다."]
    lines += ["", "## 하지 않은 것", "", "- DB 재시작: 자동 허용 목록에 없습니다.",
              f"- 같은 조치의 반복: 사건당 조치는 종류별 1회, 총 {MAX_ACTIONS}회까지입니다.", ""]
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return path


def measure_baseline(secs=10):
    windows = []
    for _ in range(secs):
        time.sleep(1)
        obs.poll()
        windows.append(obs.window(SPAN)[0])
    done = [r for r in obs.reqs.values() if r["end"]]
    if not done or any(r["result"] != "success" for r in done):
        sys.exit("평소 기준을 재는 동안 실패한 요청이 있습니다. 서비스가 정상일 때 시작하세요 (scenario.py reset).")
    full = windows[int(SPAN):]   # 처음 5초는 창이 덜 찼다
    mid = lambda xs: sorted(xs)[len(xs) // 2]  # noqa: E731
    base = {"requests_per_s": mid([w["client_to_nginx"]["requests_per_s"] for w in full]),
            "top_source_share": mid([w["client_to_nginx"]["top_source_share"] for w in full])}
    for p in PATHS:
        base[p + "_path_p95_ms"] = p95([r["ms"] for r in done if r["path"] == p])
    return base


def main():
    global obs
    obs = Observer()
    print("평소 기준 측정 중 (10초)...", flush=True)
    base = measure_baseline()
    emit("baseline", f"평소 기준: {json.dumps(base)}", baseline=base)

    mode, inc = "monitor", None      # monitor -> verify -> monitor | escalated
    streak, streak_t, quiet_until, calm = None, 0.0, 0.0, 0

    while True:
        started = time.time()
        obs.poll()
        if time.time() - obs.fresh > 3:
            emit("observation_gap", "관측 끊김: 정상 클라이언트 기록이 3초 넘게 없음. 조치하지 않고 대기")
            if mode == "verify" and time.time() - inc["verify_wall"] > VERIFY_TIMEOUT:
                escalate(inc, base, "undetermined", f"조치 후 {VERIFY_TIMEOUT}초 동안 관측이 끊겨 복구 여부를 판정하지 못함")
                mode, calm = "escalated", 0
        elif mode == "verify":
            state, after = verify_state(inc, base)
            v = judge(state, VERIFY_Q, rule_verify(after, base))
            new = obs.ended_since(inc["last_check_t"], started_after=inc["action_t"])
            inc.update(after=after, last_verdict=v)
            result = check_verdict(inc, v, state["after_action"]["enough_samples"], new, obs.now)
            emit("verification", f"검증: {show(v)} | 완료 {after['completed_requests']}, 진행 중 "
                 f"{after['in_flight_requests']}" + (" | 보류: 모델 판정과 측정값이 다름" if inc["conflict"] else ""),
                 inc, after=state["after_action"], counts=after, verdict=v, new_requests=new, conflict=inc["conflict"])
            if result == "recovered":
                emit("resolved", f"복구 확인. 감지부터 {obs.now - inc['detected_t']:.1f}초", inc,
                     seconds_from_detection=round(obs.now - inc["detected_t"], 1), actions=inc["tried"])
                # 전환 전에 주 앱으로 들어간 요청은 전환 뒤 2초(클라이언트 제한)까지 타임아웃으로 끝난다. 그 흔적이 창에서 빠질 때까지 기다린다
                drain = 2 if inc["tried"][-1] == "switch_to_standby" else 0
                mode, streak, quiet_until = "monitor", None, inc["action_t"] + SPAN + 0.5 + drain
            elif result == "not_recovered":
                action, why = next_action(inc, obs.window(SPAN)[0], base)
                if not action and inc["fault"] == "slow" and inc["tried"] == ["restart_app"]:
                    # 허용 조치를 다 썼지만 바로 이관하지 않고, 무엇을 더 확인할지 Zoro-1에 묻는다
                    inc.update(next_wall=time.time(), asked_t=None, probe=None, trail=[])
                    mode = "next"
                elif not action:
                    escalate(inc, base, "not_recovered", why)
                    mode, calm = "escalated", 0
                elif not run_action(inc, action):
                    escalate(inc, base, "action_failed", inc["actions"][-1]["result"])
                    mode, calm = "escalated", 0
            elif time.time() - inc["verify_wall"] > VERIFY_TIMEOUT:
                if inc["conflict"]:   # 제한 시간까지 모델과 측정값이 어긋난 채였다
                    escalate(inc, base, "disagreement", f"조치 후 {VERIFY_TIMEOUT}초 동안 모델 판정({v['label']})과 "
                                                        f"측정값({v['rule']})이 일치하지 않음")
                else:
                    escalate(inc, base, "undetermined", f"조치 후 {VERIFY_TIMEOUT}초 안에 복구 여부를 판정하지 못함")
                mode, calm = "escalated", 0
        elif mode == "next":
            mode, calm = next_step(inc, base), 0
        elif obs.now >= quiet_until:   # monitor / escalated: 진단
            w, meta = obs.window(SPAN)
            v = judge({"normal_baseline": base, "window_5s": w}, DIAG_Q, rule_diagnose(w, base))
            new = obs.ended_since(streak_t) if streak else 0   # 첫 이상 판정 이후 새로 끝난 요청
            emit("diagnosis", f"진단: {show(v)}", inc if mode == "escalated" else None, window=w, verdict=v,
                 new_requests=new)
            fault = v["label"] if v["label"] in POLICY and accepted(v) else None
            if fault and v["rule"] == "normal":
                # 모델은 이상이라고 했지만 측정값(실패·타임아웃·지연·연결 오류·요청량)은 정상 범위다.
                # 사용자 영향이 측정되지 않으면 사건을 열지 않는다. 정상 상태에서 앱을 재시작하던 문제를 막는 조건.
                emit("held", f"보류: 모델은 {fault} {v['p']:.2f}, 측정값은 정상 범위라 사건을 열지 않음", fault=fault, p=v["p"])
                fault = None
            if mode == "escalated":
                # 이관한 사건에는 더 조치하지 않는다 (같은 장애의 자동 조치를 반복하지 않는다).
                # 이관 뒤 새 요청이 두 경로에서 충분히 끝났고 측정값이 5회 연속 정상이면 새 사건 감시를 다시 시작한다.
                # 모델이 복구를 확인했다는 뜻도, 담당자가 처리를 끝냈다는 뜻도 아니다. 사건은 그대로 사람에게 남는다.
                calm = calm + 1 if v["rule"] == "normal" else 0
                fresh = obs.since(inc["escalated_t"])
                per_path = [sum(fresh[p + "_path"][k] for k in ("success", "failed", "timeout")) for p in PATHS]
                if calm >= 5 and fresh["completed_requests"] >= MIN_SAMPLES and min(per_path) >= MIN_NEW:
                    emit("monitoring_resumed", "측정값 정상화: 새 사건 감시 재개 (이관한 사건은 사람에게 남아 있음)", inc,
                         normal_checks=calm, completed_since_escalation=fresh["completed_requests"])
                    mode, inc, streak = "monitor", None, None
                elif not fault or fault == inc["fault"]:
                    streak = None
                elif streak != fault:
                    streak, streak_t = fault, obs.now
                elif new >= MIN_NEW and fault not in inc["alerted"]:
                    # 이관한 장애가 계속되는 중에 다른 유형이 확인됐다: 경보만 남기고 자동 조치는 하지 않는다
                    inc["alerted"].append(fault)
                    emit("alert", f"경보: 이관한 사건({inc['fault']})이 계속되는 중에 다른 장애({fault}) 감지. 자동 조치 없음", inc,
                         fault=fault, verdict=v, window=w)
            elif not fault:
                streak = None
            elif streak != fault:
                streak, streak_t = fault, obs.now                # 첫 번째 이상 판정
                emit("suspect", f"이상 판정 1회: {fault}. 새 요청으로 한 번 더 확인", fault=fault)
            elif new >= MIN_NEW:                                 # 새 요청이 쌓인 뒤 같은 판정: 사건으로 연다
                inc = {"id": time.strftime("%m%d-%H%M%S", time.localtime(obs.now)), "fault": fault,
                       "detected_t": obs.now, "diagnosis": v, "before": w, "top_source": meta["top_source"],
                       "tried": [], "actions": []}
                emit("incident", f"사건 시작: {fault}", inc, fault=fault)
                action, why = next_action(inc, w, base)
                if not action:
                    escalate(inc, base, "no_action", why)
                    mode, calm = "escalated", 0
                elif run_action(inc, action):
                    mode = "verify"
                else:
                    escalate(inc, base, "action_failed", inc["actions"][-1]["result"])
                    mode, calm = "escalated", 0
        time.sleep(max(0.0, TICK - (time.time() - started)))


if __name__ == "__main__":
    main()
