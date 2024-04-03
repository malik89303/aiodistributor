import asyncio

import pytest

from aiodistributor.distibuted_barrier import BrokenBarrierError, DistributedBarrier


async def test_barrier_wait_success(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_barrier', parties=3)

    async def participant():
        await barrier.wait()

    participants = [asyncio.create_task(participant()) for _ in range(3)]
    await asyncio.gather(*participants)


async def test_barrier_reset(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_barrier_reset', parties=2)

    async def participant():
        await barrier.wait()

    participant_task = asyncio.create_task(participant())
    await asyncio.sleep(0.1)
    await barrier.reset()

    with pytest.raises(BrokenBarrierError):
        await participant_task


async def test_barrier_abort(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_barrier_abort', parties=2)

    async def participant():
        await barrier.wait()

    participant_task = asyncio.create_task(participant())
    await asyncio.sleep(0.1)
    await barrier.abort()

    with pytest.raises(BrokenBarrierError):
        await participant_task


async def test_barrier_wait_timeout(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_barrier_timeout', parties=2)

    async def participant():
        await asyncio.wait_for(barrier.wait(), timeout=0.5)

    with pytest.raises(asyncio.TimeoutError):
        await participant()


async def test_barrier_with_single_party(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_single_party', parties=1)

    await barrier.wait()


async def test_barrier_reuse_after_reset(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_reuse_reset', parties=3)

    async def participant(barrier):
        try:
            await barrier.wait()
        except BrokenBarrierError:
            return 'reset'

    first_attempt = asyncio.create_task(participant(barrier))
    second_attempt = asyncio.create_task(participant(barrier))
    await asyncio.sleep(0.1)
    await barrier.reset()
    results = await asyncio.gather(first_attempt, second_attempt, return_exceptions=True)
    assert all(result == 'reset' for result in results)

    third_attempt = asyncio.create_task(participant(barrier))
    fourth_attempt = asyncio.create_task(participant(barrier))
    await asyncio.sleep(0.1)
    await barrier.reset()
    results = await asyncio.gather(third_attempt, fourth_attempt, return_exceptions=True)
    assert all(result == 'reset' for result in results)


async def test_barrier_multiple_use(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_reuse_reset', parties=2)

    async def participant(barrier):
        await barrier.wait()
        return True

    first_attempt = asyncio.create_task(participant(barrier))
    second_attempt = asyncio.create_task(participant(barrier))
    await asyncio.sleep(0.1)
    results = await asyncio.gather(first_attempt, second_attempt, return_exceptions=True)
    print([result for result in results])
    assert all(result is True for result in results)

    third_attempt = asyncio.create_task(participant(barrier))
    fourth_attempt = asyncio.create_task(participant(barrier))
    await asyncio.sleep(0.1)
    results = await asyncio.gather(third_attempt, fourth_attempt, return_exceptions=True)
    assert all(result is True for result in results)


async def test_barrier_abort_during_wait(isolate_redis):
    barrier = DistributedBarrier(isolate_redis, 'test_abort_wait', parties=2)

    async def participant():
        try:
            await barrier.wait()
        except BrokenBarrierError:
            return 'aborted'

    first_participant = asyncio.create_task(participant())
    second_participant = asyncio.create_task(participant())
    await asyncio.sleep(0.1)
    await barrier.abort()
    results = await asyncio.gather(first_participant, second_participant, return_exceptions=True)
    assert all(result == 'aborted' for result in results)
