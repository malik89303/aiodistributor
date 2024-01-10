from os import getenv

import pytest
from redis.asyncio.client import Redis

from aiodistributor.distributed_notifier import DistributedNotifier
from aiodistributor.distributed_waiter import DistributedWaiter

REDIS_HOST = getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(getenv('REDIS_PORT', 6379))
REDIS_USER = getenv('REDIS_USER', '')
REDIS_PASSWORD = getenv('REDIS_PASSWORD', '')


@pytest.fixture()
async def isolate_redis() -> Redis:
    redis = Redis(
        host=REDIS_HOST,
        username=REDIS_USER,
        password=REDIS_PASSWORD,
        port=REDIS_PORT,
        decode_responses=True,
    )
    await redis.initialize()
    yield redis
    await redis.flushall()
    await redis.aclose()


@pytest.fixture()
async def isolate_distributed_waiter(isolate_redis) -> DistributedWaiter:
    waiter = DistributedWaiter(isolate_redis)
    await waiter.start()
    yield waiter
    await waiter.stop()


@pytest.fixture()
async def isolate_distributed_notifier(isolate_redis) -> DistributedNotifier:
    waiter = DistributedNotifier(isolate_redis)

    yield waiter
