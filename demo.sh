#!/bin/bash
# 데모 준비: 테스트 서비스, Zoro-1 서버, 에이전트, 화면 서버를 차례로 켠다. 이미 떠 있는 것은 그대로 둔다.
# 에이전트만 항상 새로 켠다 (켤 때 10초 동안 평소 기준을 재므로, 서비스가 정상일 때 시작해야 한다).
#
#   ./demo.sh            준비
#   ./demo.sh clean      지금까지의 기록을 logs/archive/ 아래에 보관하고 비운 뒤 준비 (화면이 빈 상태로 시작)
#   ./demo.sh stop       에이전트, 모델 서버, 화면 서버, 테스트 서비스를 모두 끈다
cd "$(dirname "$0")" || exit 1
ROOT=$(pwd)
PY=$ROOT/.venv/bin/python
# 우리 프로세스는 전체 경로로 띄우고 전체 경로로만 끈다.
# 여러 사람이 같이 쓰는 서버에서 이름이 비슷한 남의 프로세스를 끄지 않기 위해서다.
AGENT=$ROOT/agent/agent.py; ZORO=$ROOT/agent/zoro_server.py; WEB=$ROOT/web/server.py
mkdir -p logs/agent/reports logs/client logs/nginx
# 기록 파일을 컨테이너보다 먼저 만들어 둔다. Linux에서는 컨테이너가 만든 파일이 관리자 소유가 돼서 비울 수 없게 된다
touch logs/client/requests.jsonl logs/nginx/access.jsonl logs/agent/events.jsonl logs/injections.jsonl

if [ "$1" = "clean" ]; then
  A=logs/archive/$(date +%Y-%m-%d_%H%M%S)
  pkill -f "$AGENT"; pkill -f "$WEB"; sleep 1    # 기록을 읽고 쓰는 쪽을 먼저 끈다
  mkdir -p "$A" && cp -R logs/agent logs/client logs/nginx "$A"/ || { echo "보관하지 못해 기록을 비우지 않았습니다."; exit 1; }
  [ -f logs/injections.jsonl ] && cp logs/injections.jsonl "$A"/
  if [ "$(wc -l < logs/agent/events.jsonl)" != "$(wc -l < "$A/agent/events.jsonl")" ]; then
    echo "보관한 기록이 원본과 달라 비우지 않았습니다."; exit 1
  fi
  # 클라이언트와 nginx가 파일을 열어 둔 채 덧붙여 쓰므로, 지우지 않고 제자리에서 비운다
  : > logs/agent/events.jsonl; : > logs/injections.jsonl; : > logs/client/requests.jsonl; : > logs/nginx/access.jsonl
  rm -f logs/agent/reports/*.md
  echo "기록을 $A 에 보관하고 비웠습니다."
fi

if [ "$1" = "stop" ]; then
  pkill -f "$AGENT"; pkill -f "$ZORO"; pkill -f "$WEB"
  docker compose down
  exit 0
fi

docker info >/dev/null 2>&1 || { echo "Docker가 꺼져 있습니다. Docker를 켠 뒤 다시 실행하세요."; exit 1; }
docker compose up -d || exit 1
"$PY" scenario.py reset >/dev/null || { echo "서비스를 정상 상태로 되돌리지 못했습니다."; exit 1; }

if ! curl -s -m 2 http://127.0.0.1:8765/ >/dev/null; then
  echo "Zoro-1 서버를 켭니다 (모델 로딩 약 10초)..."
  nohup "$PY" "$ZORO" > logs/agent/zoro_server.log 2>&1 &
  for _ in $(seq 60); do curl -s -m 2 http://127.0.0.1:8765/ >/dev/null && break; sleep 1; done
  curl -s -m 2 http://127.0.0.1:8765/ >/dev/null || { echo "Zoro-1 서버가 응답하지 않습니다. logs/agent/zoro_server.log를 확인하세요."; exit 1; }
fi

if ! curl -s -m 2 http://127.0.0.1:8090/api/live >/dev/null; then
  nohup "$PY" "$WEB" > logs/agent/web_server.log 2>&1 &
fi

pkill -f "$AGENT"; sleep 6     # 초기화 직후의 흔적이 5초 창에서 빠질 때까지 잠깐 기다린다
nohup "$PY" "$AGENT" > logs/agent/agent.out 2>&1 &
echo "에이전트가 평소 기준을 재는 중 (10초)..."
sleep 13
sed -n '2p' logs/agent/agent.out
pgrep -f "$AGENT" >/dev/null || { echo "에이전트가 시작되지 않았습니다. logs/agent/agent.out을 확인하세요."; exit 1; }
echo
echo "준비 완료. 화면: http://localhost:8090/#live   (기록 재생: http://localhost:8090/)"
echo "장애 주입: .venv/bin/python scenario.py orders_fault | db_delay | app_down      되돌리기: .venv/bin/python scenario.py reset"
