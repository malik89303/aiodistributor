import asyncio
import os
import random
import time

import ujson
from aiohttp import web
from redis.asyncio import Redis

from aiodistributor.distributed_bounded_semaphore import DistributedBoundedSemaphore

EXEC_STATS = []
NODE = random.randint(0, 999)


async def handle_acquire(request):
    redis_client = Redis(host='redis')
    semaphore = DistributedBoundedSemaphore(redis_client, 'test_bounded_semaphore', 1000)

    start_time = time.perf_counter()
    try:
        await semaphore.acquire()
        acquired = True
    except asyncio.TimeoutError:
        acquired = False
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'acquire',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
            'success': acquired,
        }
    )

    await redis_client.append('bounded_semaphore_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Bounded semaphore acquired by node {NODE}. Success: {acquired}')


async def handle_release(request):
    redis_client = Redis(host='redis')
    semaphore = DistributedBoundedSemaphore(redis_client, 'test_bounded_semaphore', 1000)

    start_time = time.perf_counter()
    await semaphore.release()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'release',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('bounded_semaphore_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Bounded semaphore released by node {NODE}.')


app = web.Application()
app.add_routes([web.get('/acquire', handle_acquire)])
app.add_routes([web.get('/release', handle_release)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    web.run_app(app, port=port)
