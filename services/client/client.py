"""정상 사용자 모사. 두 경로를 일정 속도로 호출하고, 요청마다 시작과 끝을 한 줄씩 기록한다.

시작 줄을 따로 남기는 이유: 아직 응답을 기다리는 요청(진행 중)을 셀 수 있어야 하기 때문.
"""
import asyncio
import json
import os
import time

import aiohttp

TARGET = os.environ.get("TARGET", "http://nginx")
RATE = float(os.environ.get("RATE", "1"))        # 경로별 초당 요청 수 (복제본 하나당)
TIMEOUT = float(os.environ.get("TIMEOUT", "2"))  # 이 시간 안에 응답이 없으면 타임아웃
PATHS = {"db": "/api/orders", "light": "/api/ping"}

out = open("/logs/requests.jsonl", "a", buffering=1)


def log(**row):
    out.write(json.dumps(row) + "\n")


async def one(session, rid, name):
    start = time.time()
    log(ev="start", id=rid, path=name, t=start)
    status = None
    try:
        async with session.get(TARGET + PATHS[name], timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
            await r.read()
            status = r.status
            result = "success" if r.status == 200 else "failed"
    except asyncio.TimeoutError:
        result = "timeout"
    except aiohttp.ClientError:
        result = "failed"
    end = time.time()
    log(ev="end", id=rid, path=name, t=end, start=start, result=result, status=status,
        ms=round((end - start) * 1000, 1))


async def main():
    me, n = f"{os.uname().nodename}-{int(time.time())}", 0   # 복제본끼리도, 재시작해도 겹치지 않는 요청 번호
    async with aiohttp.ClientSession() as session:
        tasks = set()
        while True:
            for name in PATHS:
                n += 1
                t = asyncio.create_task(one(session, f"{me}-{n}", name))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
            await asyncio.sleep(1 / RATE)


asyncio.run(main())
