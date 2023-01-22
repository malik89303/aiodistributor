import asyncio

import pytest

from aiodistributor.distributed_sliding_counter import DistributedSlidingCounter


@pytest.mark.parametrize(
    'values, expected_count',
    [
        (range(1, 2), 1),
        (range(1, 3), 2),
        (range(1, 6), 5),
        (range(1, 11), 10),
        (range(1, 51), 50),
        (range(1, 101), 100),
        (range(1, 112), 111),
        (range(1, 201), 200),
    ]
)
async def test_count(isolate_redis, values, expected_count):
    counter = DistributedSlidingCounter(isolate_redis, 'aboba', 200)
    res = await asyncio.gather(*[counter.increase() for _ in values])

    assert await counter.count() == expected_count
    assert set(res) == set(values)

    await asyncio.sleep(0.3)
    assert await counter.count() == 0


@pytest.mark.parametrize('inserted_count', list(range(10)))
async def test_reset(isolate_redis, inserted_count):
    counter = DistributedSlidingCounter(isolate_redis, 'aboba', 10 ** 10)
    await asyncio.gather(*[counter.increase() for _ in range(inserted_count)])
    assert await counter.count() == inserted_count

    res = await counter.reset()
    assert res == 0

    assert await counter.count() == 0


async def test_increase_and_count(isolate_redis):
    counter = DistributedSlidingCounter(isolate_redis, 'aboba', 10)

    assert (await counter.increase()) == 1
    await asyncio.sleep(0.02)
    assert (await counter.count()) == 0

    assert (await counter.increase()) == 1
    assert (await counter.increase()) == 2
    assert (await counter.increase()) == 3
    assert (await counter.increase()) == 4
    await asyncio.sleep(0.02)
    assert (await counter.count()) == 0
