import asyncio
import os
import random
import time

import ujson
from redis.asyncio import Redis

from aiodistributor.distributed_sliding_counter import DistributedSlidingCounter

EXEC_STATS = []
NODE = random.randint(0, 100)


async def measure_performance(max_value: int) -> None:
    redis_client = Redis(host='host.docker.internal')

    counter_key = 'counter_key'
    counter = DistributedSlidingCounter(redis_client, key=counter_key)

    for _ in range(max_value):
        incr_start = time.perf_counter()
        val = await counter.increase()
        incr_end = time.perf_counter()
        ret_start = time.perf_counter()
        current_count = await counter.count()
        ret_end = time.perf_counter()
        EXEC_STATS.append(
            {
                'node_id': NODE,
                'incr_start': incr_start,
                'incr_end': incr_end,
                'ret_start': ret_start,
                'ret_end': ret_end,
                'incr_val': val,
                'counter_val': current_count
            }
        )

    await redis_client.append('stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()


if __name__ == '__main__':
    max_value = int(os.getenv('MAX_VALUE', 100))
    asyncio.run(measure_performance(max_value))
    print(EXEC_STATS)
