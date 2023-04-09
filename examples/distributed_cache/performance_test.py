import asyncio
import os
import random
import time

import ujson
from redis.asyncio import Redis

from aiodistributor.distributed_cache import distributed_cache

EXEC_STATS = []
NODE = random.randint(0, 999)


async def measure_performance(max_val) -> None:

    redis_client = Redis(host='host.docker.internal')

    @distributed_cache(redis_client)
    async def performance_test_function(val: int) -> int:
        await asyncio.sleep(1)
        return val * 2

    vals = list(range(max_val))
    random.shuffle(vals)
    for i in vals:
        start_time = time.perf_counter()
        await performance_test_function(i)
        ended_at = time.perf_counter()
        EXEC_STATS.append(
            {
                'val': i,
                'started_at': start_time,
                'ended_at': ended_at,
                'node_id': NODE,
            }
        )

    await redis_client.append('stats', ujson.dumps(EXEC_STATS))
    await redis_client.close()


if __name__ == '__main__':
    max_val = int(os.getenv('MAX_VAL', 30))
    asyncio.run(measure_performance(max_val))
    print(EXEC_STATS)
