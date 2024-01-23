import asyncio

import pytest

from aiodistributor.distibuted_semaphore import DistributedSemaphore


async def test_successful_semaphore_acquisition(isolate_redis):
    semaphore = DistributedSemaphore(isolate_redis, 'test_semaphore', value=2)
    acquired_first = await semaphore.acquire()
    assert acquired_first
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1

    acquired_second = await semaphore.acquire()
    assert acquired_second
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 0
    assert await semaphore.locked() is True

    await semaphore.release()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1

    await semaphore.release()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 2
    assert await semaphore.locked() is False


async def test_failed_semaphore_acquisition_due_to_timeout(isolate_redis):
    semaphore1 = DistributedSemaphore(isolate_redis, 'test_semaphore', value=1, acquire_timeout=0.1)
    semaphore2 = DistributedSemaphore(isolate_redis, 'test_semaphore', value=1, acquire_timeout=0.1)

    await semaphore1.acquire()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 0

    with pytest.raises(asyncio.TimeoutError):
        await semaphore2.acquire()

    await semaphore1.release()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1


async def test_semaphore_release(isolate_redis):
    semaphore = DistributedSemaphore(isolate_redis, 'test_semaphore', value=1)
    await semaphore.acquire()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 0

    await semaphore.release()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1
    assert await semaphore.locked() is False


async def test_context_manager_usage(isolate_redis):
    async with DistributedSemaphore(isolate_redis, 'test_semaphore', value=1) as semaphore:
        assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 0
        assert await semaphore.locked() is True
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1
    assert await semaphore.locked() is False


async def test_locked_method(isolate_redis):
    semaphore = DistributedSemaphore(isolate_redis, 'test_semaphore', value=1)
    assert await semaphore.locked() is False
    await semaphore.acquire()
    assert await semaphore.locked() is True
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 0

    await semaphore.release()
    assert await semaphore.locked() is False
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1


async def test_semaphore_release_without_acquisition(isolate_redis):
    semaphore = DistributedSemaphore(isolate_redis, 'test_semaphore', value=0)
    assert await semaphore.locked() is True

    await semaphore.release()

    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1


async def test_semaphore_initialization_with_negative_value(isolate_redis):
    with pytest.raises(ValueError):
        DistributedSemaphore(isolate_redis, 'test_semaphore', value=-1)


async def test_semaphore_release_when_max_count_exceeded(isolate_redis):
    semaphore = DistributedSemaphore(isolate_redis, 'test_semaphore', value=1)
    assert await semaphore.locked() is False

    await semaphore.acquire()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 0

    await semaphore.release()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 1

    await semaphore.release()
    assert int(await isolate_redis.get('distributed_semaphore_test_semaphore')) == 2
