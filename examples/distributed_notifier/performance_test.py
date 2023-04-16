import asyncio
import json
import time
from os import getenv
from random import randint

import ujson
from redis.asyncio import Redis

from aiodistributor.distributed_notifier import DistributedNotifier

EXEC_STATS = []
NODE = randint(0, 100)


async def simulate_workers(number_of_subscribers: int, number_of_groups: int = 4):
    redis_client = Redis(host='host.docker.internal', decode_responses=True)
    notifier = DistributedNotifier(redis=redis_client, )

    async def perform_message_reading(notifier: DistributedNotifier, group_id: int, worker_id: int) -> None:
        async for raw_message in notifier.get_message(stream_id=f'stream_{group_id}'):
            if raw_message == 'STOP':
                return
            message_data = json.loads(raw_message)

            EXEC_STATS.append(
                message_data | {
                    'receiving_time': time.time(),
                    'group_id': group_id,
                    'node_id': NODE,
                    'worker_id': worker_id,
                }
            )

    await asyncio.gather(
        *[
            perform_message_reading(
                notifier=notifier,
                group_id=group_id,
                worker_id=worker_id,
            ) for group_id in range(number_of_groups) for worker_id in range(number_of_subscribers)
        ]
    )

    await redis_client.append('stats', ujson.dumps(EXEC_STATS))
    await redis_client.close()


async def simulate_sender(number_of_messages: int = 10, number_of_groups: int = 4) -> None:
    redis_client = Redis(host='localhost', decode_responses=True)

    notifier = DistributedNotifier(redis=redis_client)

    for j in range(number_of_groups):
        for i in range(number_of_messages):
            await notifier.add_message_to_stream_if_newer(
                stream_id=f'stream_{j}',
                message=ujson.dumps(
                    {
                        'sent_at': time.time(),
                        'message_id': f'{i}_{j}'
                    }
                )
            )

        await notifier.add_message_to_stream_if_newer(
            stream_id=f'stream_{j}',
            message='STOP'
        )

    await redis_client.close()


if __name__ == '__main__':
    number_of_subscribers = int(getenv('NUMBER_OF_SUBSCRIBERS', 25))
    asyncio.run(simulate_workers(number_of_subscribers))
