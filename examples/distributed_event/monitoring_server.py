import asyncio
import os
import time

import aiohttp
import ujson
from aiohttp import web
from redis.asyncio import Redis

EXEC_STATS = []


async def fetch_url(session: aiohttp.ClientSession, url):
    async with session.get(url, timeout=1) as response:
        start_time = time.perf_counter()
        await response.text()
        end_time = time.perf_counter()
        return start_time, end_time


async def gather_stats(request):
    server_urls = [
        'http://server1:8081/set_event',
        'http://server1:8081/wait_event',
        'http://server2:8082/set_event',
        'http://server2:8082/wait_event',
        'http://server3:8083/set_event',
        'http://server3:8083/wait_event',
        'http://server4:8084/set_event',
        'http://server4:8084/wait_event',
        'http://server5:8085/set_event',
        'http://server5:8085/wait_event',
    ]

    num_requests = 200  # Number of requests to send to each server

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(num_requests):
            for url in server_urls:
                tasks.append(fetch_url(session, url))
        results = await asyncio.gather(*tasks)

    for result, url in zip(results, server_urls * num_requests):
        start_time, end_time = result
        EXEC_STATS.append(
            {
                'server': url,
                'started_at': start_time,
                'ended_at': end_time,
            }
        )

    redis_client = Redis(host='redis')
    await redis_client.append('accumulated_event_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=ujson.dumps(EXEC_STATS, indent=4))


app = web.Application()
app.add_routes([web.get('/gather_stats', gather_stats)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8090))
    web.run_app(app, port=port)
