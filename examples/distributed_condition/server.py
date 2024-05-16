import os
import random
import time

import ujson
from aiohttp import web
from redis.asyncio import Redis

from aiodistributor.distributed_condition import DistributedCondition

EXEC_STATS = []
NODE = random.randint(0, 999)


async def handle_wait(request):
    redis_client = Redis(host='redis')
    condition = DistributedCondition(redis_client, 'test_condition')

    start_time = time.perf_counter()
    async with condition._lock:
        await condition.wait()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'wait',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('condition_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Condition wait by node {NODE}')


async def handle_notify(request):
    redis_client = Redis(host='redis')
    condition = DistributedCondition(redis_client, 'test_condition')

    start_time = time.perf_counter()
    async with condition._lock:
        await condition.notify()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'notify',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('condition_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Condition notify by node {NODE}')


async def handle_notify_all(request):
    redis_client = Redis(host='redis')
    condition = DistributedCondition(redis_client, 'test_condition')

    start_time = time.perf_counter()
    async with condition._lock:
        await condition.notify_all()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'notify_all',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('condition_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Condition notify all by node {NODE}')


app = web.Application()
app.add_routes([web.get('/wait', handle_wait)])
app.add_routes([web.get('/notify', handle_notify)])
app.add_routes([web.get('/notify_all', handle_notify_all)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    web.run_app(app, port=port)
