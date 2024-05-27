import asyncio
import os
import random
import time

import ujson
from aiohttp import web
from redis.asyncio import Redis

from aiodistributor.distributed_lock import DistributedLock

EXEC_STATS = []
NODE = random.randint(0, 999)


async def handle(_: web.Request) -> web.Response:
    redis_client = Redis(host='host.docker.internal')
    lock = DistributedLock(redis_client, f'test_lock{NODE}')

    start_time = time.perf_counter()
    async with lock:
        await asyncio.sleep(0.1)  # Simulate some work while holding the lock
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('lock_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Lock acquired and released by node {NODE}.')


app = web.Application()
app.add_routes([web.get('/', handle)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    web.run_app(app, port=port)
