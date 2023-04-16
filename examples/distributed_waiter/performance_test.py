import asyncio
import itertools
import time
from collections import defaultdict
from os import getenv
from random import shuffle

import ujson
from redis.asyncio import Redis

from aiodistributor.distributed_waiter import DistributedWaiter

EXEC_STATS = defaultdict(dict)


async def simulate_workers(number_of_messages: int = 10):
    redis_client = Redis(host='host.docker.internal', decode_responses=True)
    notifier = DistributedWaiter(redis=redis_client)
    await notifier.start()

    async with redis_client.lock(name='counter'):
        node_id = await redis_client.get('node_id')
        if node_id is None:
            node_id = 0
        else:
            node_id = int(node_id) + 1
        await redis_client.set('node_id', node_id)

    async def perform_message_reading(notifier: DistributedWaiter, key: str) -> None:
        waiter = await notifier.create_waiter(key, timeout=10000, expire=10000)
        await waiter
        EXEC_STATS[key]['received_at'] = time.time()

    await asyncio.gather(
        *[
            perform_message_reading(
                notifier=notifier,
                key=f'{node_id}_{i}',
            ) for i in range(number_of_messages)
        ]
    )

    await notifier.stop()
    await redis_client.append('stats', ujson.dumps(EXEC_STATS))
    await redis_client.close()


async def simulate_sender(number_of_workers: int, number_of_messages: int = 10) -> None:
    redis_client = Redis(host='localhost', decode_responses=True)
    await redis_client.initialize()
    notifier = DistributedWaiter(redis=redis_client)
    await notifier.start()
    s = list(itertools.product(range(number_of_messages), range(number_of_workers)))
    shuffle(s)
    for i, j in s:
        print(i, j)
        key = f'{j}_{i}'
        EXEC_STATS[key]['sent_at'] = time.time()
        await notifier.notify(key)

    await redis_client.append('stats', ujson.dumps(EXEC_STATS))
    await notifier.stop()
    await redis_client.close()


if __name__ == '__main__':
    number_of_messages = int(getenv('NUMBER_OF_MESSAGES', 10))
    asyncio.run(simulate_workers(number_of_messages))
