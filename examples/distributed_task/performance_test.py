import asyncio
import os
import random
import time

import ujson
from redis.asyncio import Redis

from aiodistributor.distributed_task import DistributedTask

EXEC_STATS = []
NODE = random.randint(0, 999)


async def performance_test_function() -> None:
    start_time = time.perf_counter()
    await asyncio.sleep(1)
    ended_at = time.perf_counter()
    global EXEC_STATS
    EXEC_STATS.append(
        {
            'started_at': start_time,
            'ended_at': ended_at,
            'node_id': NODE,
        }
    )


async def measure_performance(period: float) -> None:
    redis_client = Redis(host='host.docker.internal')

    task = DistributedTask(
        name='performance_test_task',
        task_func=performance_test_function,
        redis=redis_client,
        task_period=1.0,
        locker_timeout=5 * 60.0,
        logging_period=5 * 60.0,
    )

    await task.start()
    await asyncio.sleep(period)
    await task.stop(timeout=0.)
    await redis_client.append('stats', ujson.dumps(EXEC_STATS))
    await redis_client.aclose()


if __name__ == '__main__':
    period = float(os.getenv('PERIOD', 5))
    asyncio.run(measure_performance(period))
    print(EXEC_STATS)
