import asyncio
import os
import time

import aiohttp
import ujson
from aiohttp import web
from redis.asyncio import Redis

EXEC_STATS = []


async def fetch_url(session, url):
    async with session.get(url) as response:
        start_time = time.perf_counter()
        await response.text()
        end_time = time.perf_counter()
        return start_time, end_time


async def gather_stats(request):
    server_urls = [
        'http://server1:8081/wait',
        'http://server1:8081/notify',
        'http://server1:8081/notify_all',
        'http://server2:8082/wait',
        'http://server2:8082/notify',
        'http://server2:8082/notify_all',
        'http://server3:8083/wait',
        'http://server3:8083/notify',
        'http://server3:8083/notify_all',
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in server_urls]
        results = await asyncio.gather(*tasks)

    for result, url in zip(results, server_urls):
        start_time, end_time = result
        EXEC_STATS.append(
            {
                'server': url,
                'started_at': start_time,
                'ended_at': end_time,
            }
        )

    redis_client = Redis(host='redis')
    await redis_client.append('accumulated_condition_stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()

    return web.Response(text=ujson.dumps(EXEC_STATS, indent=4))


app = web.Application()
app.add_routes([web.get('/gather_stats', gather_stats)])

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8090))
    web.run_app(app, port=port)
