from os import getenv

import pytest
from redis.asyncio.client import Redis

from aiodistributor.distributed_futures import RedisCondition

REDIS_HOST = getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(getenv('REDIS_PORT', 6379))
REDIS_USER = getenv('REDIS_USER', '')
REDIS_PASSWORD = getenv('REDIS_PASSWORD', '')


@pytest.fixture()
async def isolate_redis(event_loop) -> Redis:
    redis = Redis(
        host=REDIS_HOST,
        username=REDIS_USER,
        password=REDIS_PASSWORD,
        port=REDIS_PORT,
    )
    await redis.initialize()
    yield redis
    await redis.flushall()
    await redis.close()


@pytest.fixture()
async def isolate_redis_condition(isolate_redis, event_loop) -> Redis:
    condition = RedisCondition(isolate_redis, loop=event_loop)
    await condition.start()
    yield condition
    await condition.stop()
