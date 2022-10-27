import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis

from aiodistributor.distributed_futures import RedisCondition

ID = str(uuid.uuid4())


async def test_notify_waiter_without_request(isolate_redis_condition: RedisCondition):
    result = await isolate_redis_condition.notify(ID)
    assert result is False


async def test_notify_waiter(isolate_redis_condition: RedisCondition):
    res = await isolate_redis_condition.create_waiter(ID, 300, 3)
    task = asyncio.create_task(res)
    await asyncio.sleep(0)  # it is used to run task now

    notifying_result = await isolate_redis_condition.notify(ID)
    assert notifying_result is True

    waiting_result = await task
    assert waiting_result is True


async def test_wait_response_timeout(isolate_redis_condition: RedisCondition):
    waiter = await isolate_redis_condition.create_waiter(ID, 300, 0)
    with pytest.raises(asyncio.TimeoutError):
        await waiter


async def test_wait_response_without_consumer_subscription(isolate_redis_condition: RedisCondition):
    await isolate_redis_condition._redis.set(ID, 'aboba')
    waiting_result = await isolate_redis_condition.notify(ID)
    assert waiting_result is False


async def test_condition_healthcheck(isolate_redis: Redis, event_loop):
    redis_condition = RedisCondition(
        isolate_redis, event_loop, healthcheck_timeout=99999
    )
    await redis_condition.start()
    healthcheck_result = await redis_condition._healthcheck()
    assert healthcheck_result is True

    await redis_condition.stop()


async def test_condition_healthcheck_no_connection(isolate_redis: Redis, event_loop):
    redis_condition = RedisCondition(isolate_redis, event_loop, healthcheck_timeout=99)
    await redis_condition.start()

    redis_condition._pubsub.connection = None
    healthcheck_result = await redis_condition._healthcheck()
    assert healthcheck_result is False

    await redis_condition.stop()


# TODO слишком долго проходит тест
async def test_condition_healthcheck_no_ping_response(isolate_redis: Redis, event_loop):
    redis_condition = RedisCondition(isolate_redis, event_loop, healthcheck_timeout=99)
    await redis_condition.start()

    await redis_condition._pubsub.unsubscribe(redis_condition._channel_name)
    healthcheck_result = await redis_condition._healthcheck()
    assert healthcheck_result is False

    await redis_condition.stop()


async def test_run_healthcheck(isolate_redis: Redis, event_loop):
    redis_condition = RedisCondition(isolate_redis, event_loop, healthcheck_timeout=0.1)

    redis_condition.restart = AsyncMock()
    redis_condition._healthcheck = AsyncMock(return_value=False)
    await redis_condition.start()
    await asyncio.sleep(0.2)
    assert redis_condition.restart.call_count == 1

    await redis_condition.stop()


async def test_restart(isolate_redis_condition: RedisCondition):
    await isolate_redis_condition.restart()
    healthcheck_result = await isolate_redis_condition._healthcheck()
    assert healthcheck_result is True


async def test_double_start(isolate_redis_condition: RedisCondition):
    with pytest.raises(ValueError):
        await isolate_redis_condition.start()


async def test_double_stop(isolate_redis: Redis):
    condition = RedisCondition(isolate_redis)

    with pytest.raises(ValueError):
        await condition.stop()

    await condition.start()
    await condition.stop()

    with pytest.raises(ValueError):
        await condition.stop()


async def test_uninitialized_healthcheck():
    condition = RedisCondition(None)

    with pytest.raises(ValueError):
        await condition._healthcheck()
