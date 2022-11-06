import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis

from aiodistributor.distributed_waiter import DistributedWaiter

ID = str(uuid.uuid4())


async def test_notify_waiter_without_request(isolate_distributed_waiter):
    result = await isolate_distributed_waiter.notify(ID)
    assert result is False


async def test_notify_waiter(isolate_distributed_waiter):
    waiter = await isolate_distributed_waiter.create_waiter(
        key=ID,
        timeout=3,
        expire=4,
    )

    notifying_result = await isolate_distributed_waiter.notify(ID)
    assert notifying_result is True

    waiting_result = await waiter
    assert waiting_result is True


async def test_wait_response_timeout(isolate_distributed_waiter):
    waiter = await isolate_distributed_waiter.create_waiter(
        key=ID,
        timeout=0,
        expire=3,
    )
    with pytest.raises(asyncio.TimeoutError):
        await waiter


async def test_wait_response_without_consumer_subscription(isolate_distributed_waiter):
    await isolate_distributed_waiter._redis.set(ID, 'aboba')
    result = await isolate_distributed_waiter.notify(ID)
    assert result is False


async def test_healthcheck(isolate_distributed_waiter):
    healthcheck_result = await isolate_distributed_waiter._healthcheck()
    assert healthcheck_result is None


async def test_healthcheck_no_connection(isolate_distributed_waiter):
    isolate_distributed_waiter._pubsub.connection = None
    with pytest.raises(ValueError):
        await isolate_distributed_waiter._healthcheck()


async def test_healthcheck_no_ping_response(isolate_redis: Redis):
    waiter = DistributedWaiter(isolate_redis, healthcheck_period=3, healthcheck_timeout=0.1)
    await waiter.start()

    await waiter._pubsub.unsubscribe(waiter._channel_name)
    with pytest.raises(asyncio.TimeoutError):
        await waiter._healthcheck()

    await waiter.stop()


async def test_run_healthcheck(isolate_redis: Redis):
    waiter = DistributedWaiter(isolate_redis, healthcheck_period=0.05)

    waiter.restart = AsyncMock()
    waiter._healthcheck = AsyncMock(side_effect=Exception)
    await waiter.start()
    await asyncio.sleep(0.1)
    assert waiter.restart.call_count == 1

    await waiter.stop()
    assert waiter._futures == {}


async def test_restart(isolate_distributed_waiter):
    await isolate_distributed_waiter.restart()
    healthcheck_result = await isolate_distributed_waiter._healthcheck()
    assert healthcheck_result is None


async def test_double_start(isolate_distributed_waiter):
    with pytest.raises(ValueError):
        await isolate_distributed_waiter.start()


async def test_double_stop(isolate_redis: Redis):
    waiter = DistributedWaiter(isolate_redis)

    with pytest.raises(ValueError):
        await waiter.stop()

    await waiter.start()
    await waiter.stop()

    with pytest.raises(ValueError):
        await waiter.stop()


async def test_uninitialized_healthcheck():
    waiter = DistributedWaiter(None)

    with pytest.raises(ValueError):
        await waiter._healthcheck()

    assert waiter._futures == {}
