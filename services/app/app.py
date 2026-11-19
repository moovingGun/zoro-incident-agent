"""대상 앱. 경로 두 개: DB를 쓰는 /api/orders, DB 없이 응답하는 /api/ping."""
import asyncio
import os

import asyncpg
from aiohttp import web


async def ping(request):
    return web.json_response({"ok": True})


# 장애 주입용: 이 파일이 있으면 이 앱의 주문 조회만 멈춘다. 컨테이너 안에 있어 앱을 재시작해도 남는다 (scenario.py orders_fault)
FAULT_FLAG = "/tmp/orders_fault"


async def orders(request):
    if os.path.exists(FAULT_FLAG):
        await asyncio.sleep(3)
    async with request.app["pool"].acquire() as conn:
        rows = await conn.fetch("SELECT * FROM recent_orders()")
    return web.json_response({"count": len(rows)})


async def connect(app):
    for _ in range(60):   # DB가 막 뜨는 중일 수 있다
        try:
            app["pool"] = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=20)
            return
        except (OSError, asyncpg.PostgresError):
            await asyncio.sleep(0.5)
    raise RuntimeError("database not reachable")


app = web.Application()
app.on_startup.append(connect)
app.add_routes([web.get("/api/ping", ping), web.get("/api/orders", orders)])
web.run_app(app, port=8000, shutdown_timeout=1)
