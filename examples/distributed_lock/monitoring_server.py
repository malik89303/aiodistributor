import asyncio
import os
import time

import ujson
from aiohttp import ClientSession, web
from redis.asyncio import Redis

EXEC_STATS = []


async def fetch_url(session, url):
    async with session.get(url) as response:
        start_time = time.perf_counter()
        await response.text()
        end_time = time.perf_counter()
        return start_time, end_time


async def gather_stats(request: web.Request) -> web.Response:
    server_urls = [
        'http://server1:8081/',
        'http://server2:8082/',
        'http://server3:8083/',
        'http://server4:8084/',
        'http://server5:8085/',
    ]

    num_requests = 1000  # Number of requests to send to each server

    async with ClientSession() as session:
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
    await redis_client.append('accumulated_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=ujson.dumps(EXEC_STATS, indent=4))


app = web.Application()
app.add_routes([web.get('/gather_stats', gather_stats)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8090))
    web.run_app(app, port=port)
