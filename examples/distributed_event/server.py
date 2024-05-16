import os
import random
import time

import ujson
from aiohttp import web
from redis.asyncio import Redis

from aiodistributor.distributed_event import DistributedEvent

EXEC_STATS = []
NODE = random.randint(0, 999)


async def handle_set_event(request):
    redis_client = Redis(host='redis')
    event = DistributedEvent(redis_client, 'test_event')

    start_time = time.perf_counter()
    await event.set()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'set',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('event_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Event set by node {NODE}.')


async def handle_wait_event(request):
    redis_client = Redis(host='redis')
    event = DistributedEvent(redis_client, 'test_event')

    start_time = time.perf_counter()
    await event.wait()
    ended_at = time.perf_counter()

    EXEC_STATS.append(
        {
            'action': 'wait',
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )

    await redis_client.append('event_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=f'Event waited by node {NODE}.')


app = web.Application()
app.add_routes([web.get('/set_event', handle_set_event)])
app.add_routes([web.get('/wait_event', handle_wait_event)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    web.run_app(app, port=port)
