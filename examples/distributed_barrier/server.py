import os
import random
import time

import ujson
from aiohttp import web
from redis.asyncio import Redis

from aiodistributor.distributed_barrier import BrokenBarrierError, DistributedBarrier

EXEC_STATS = []
NODE = random.randint(0, 999)


async def handle_wait(request):
    redis_client = Redis(host='redis', decode_responses=True)
    barrier = DistributedBarrier(redis_client, 'test_barrier', 5)

    start_time = time.perf_counter()
    try:
        await barrier.wait()
        success = True
    except BrokenBarrierError:
        success = False
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'wait',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
            'success': success,
        }
    )

    await redis_client.append('barrier_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Barrier wait by node {NODE}. Success: {success}')


async def handle_reset(request):
    redis_client = Redis(host='redis', decode_responses=True)
    barrier = DistributedBarrier(redis_client, 'test_barrier', 5)

    start_time = time.perf_counter()
    await barrier.reset()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'reset',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('barrier_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Barrier reset by node {NODE}')


async def handle_abort(request):
    redis_client = Redis(host='redis', decode_responses=True)
    barrier = DistributedBarrier(redis_client, 'test_barrier', 5)

    start_time = time.perf_counter()
    await barrier.abort()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'abort',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('barrier_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Barrier aborted by node {NODE}')


app = web.Application()
app.add_routes([web.get('/wait', handle_wait)])
app.add_routes([web.get('/reset', handle_reset)])
app.add_routes([web.get('/abort', handle_abort)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    web.run_app(app, port=port)
