import asyncio

import pytest

from aiodistributor.distributed_lock import DistributedLock


async def test_successful_lock_acquisition(isolate_redis):
    lock = DistributedLock(isolate_redis, 'test_lock')
    acquired = await lock.acquire()
    assert acquired
    assert await lock.locked()
    assert await isolate_redis.get('test_lock') is not None
    await lock.release()
    assert await isolate_redis.get('test_lock') is None


async def test_failed_lock_acquisition_due_to_timeout(isolate_redis):
    lock1 = DistributedLock(isolate_redis, 'test_lock', acquire_timeout=0.1)
    lock2 = DistributedLock(isolate_redis, 'test_lock', acquire_timeout=0.1)

    await lock1.acquire()
    assert await isolate_redis.get('test_lock') == lock1._token
    with pytest.raises(asyncio.TimeoutError):
        await lock2.acquire()

    await lock1.release()
    assert await isolate_redis.get('test_lock') is None


async def test_lock_release(isolate_redis):
    lock = DistributedLock(isolate_redis, 'test_lock')
    await lock.acquire()
    assert await isolate_redis.get('test_lock') == lock._token
    await lock.release()
    assert await isolate_redis.get('test_lock') is None


async def test_context_manager(isolate_redis):
    async with DistributedLock(isolate_redis, 'test_lock') as lock:
        assert await lock.locked()
        assert await isolate_redis.get('test_lock') == lock._token
    assert not await lock.locked()
    assert await isolate_redis.get('test_lock') is None


async def test_locked_method(isolate_redis):
    lock = DistributedLock(isolate_redis, 'test_lock')
    assert not await lock.locked()
    assert await isolate_redis.get('test_lock') is None
    await lock.acquire()
    assert await lock.locked()
    assert await isolate_redis.get('test_lock') == lock._token
    await lock.release()
    assert not await lock.locked()
    assert await isolate_redis.get('test_lock') is None


async def test_multiple_instances(isolate_redis):
    lock1 = DistributedLock(isolate_redis, 'test_lock', acquire_timeout=0.1)
    lock2 = DistributedLock(isolate_redis, 'test_lock', acquire_timeout=0.1)

    async def try_acquire(lock):
        try:
            return await lock.acquire()
        except asyncio.TimeoutError:
            return False

    acquired1, acquired2 = await asyncio.gather(
        try_acquire(lock1),
        try_acquire(lock2)
    )

    assert acquired1 != acquired2
    token = await isolate_redis.get('test_lock')
    if acquired1:
        assert token == lock1._token
        await lock1.release()
    elif acquired2:
        assert token == lock2._token
        await lock2.release()

    assert await isolate_redis.get('test_lock') is None
