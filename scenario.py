"""장애 주입과 초기화. 주입 시각은 logs/injections.jsonl에 남긴다 (사후 평가용, 에이전트에는 주지 않는다).

    .venv/bin/python scenario.py app_down          # 주 앱 강제 종료
    .venv/bin/python scenario.py db_delay [ms]     # DB 쿼리 지연 (기본 3000ms)
    .venv/bin/python scenario.py orders_fault      # 주 앱에서만 주문 조회가 멈춤 (재시작해도 남는다, 예비 앱은 정상)
    .venv/bin/python scenario.py reset             # 전부 정상으로
"""
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True, text=True)


def set_delay(ms):
    compose("exec", "-T", "db", "psql", "-U", "postgres", "-d", "shop", "-c", f"UPDATE chaos SET delay_ms={int(ms)}")


def record(kind, **extra):
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    with open(os.path.join(ROOT, "logs", "injections.jsonl"), "a") as f:
        f.write(json.dumps({"t": time.time(), "inject": kind, **extra}) + "\n")
    print(f"{time.strftime('%H:%M:%S')} {kind} {extra or ''}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "app_down":
        record("app_down")
        compose("kill", "app")
    elif cmd == "db_delay":
        ms = int(sys.argv[2]) if len(sys.argv) > 2 else 3000
        record("db_delay", ms=ms)
        set_delay(ms)
    elif cmd == "orders_fault":
        record("orders_fault")
        compose("exec", "-T", "app", "touch", "/tmp/orders_fault")
    elif cmd == "reset":
        set_delay(0)
        compose("up", "-d", "app")
        compose("exec", "-T", "app", "rm", "-f", "/tmp/orders_fault")
        for name in ("blocked.conf", "upstream.conf"):   # 출발지 제한 해제, 트래픽을 주 앱으로 되돌림
            open(os.path.join(ROOT, "services/nginx/dynamic", name), "w").close()
        compose("exec", "-T", "nginx", "nginx", "-s", "reload")
        record("reset")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
