import asyncio

import pytest

from aiodistributor.distributed_task import DistributedTask


async def task_for_test(mutable_list: list[int]) -> None:
    mutable_list.append(1)


async def long_task_for_test(mutable_list: list[int]) -> None:
    await asyncio.sleep(600)
    mutable_list.append(1)


async def test_stop_distributed_task_with_big_sleep(isolate_redis):
    mutable_list = []
    task = DistributedTask(
        name='test_interval_task',
        task_period=60,
        locker_timeout=120,
        task_func=task_for_test,
        redis=isolate_redis,
        mutable_list=mutable_list,
    )

    await task.start()
    await asyncio.sleep(0.1)
    await task.stop(0.)

    assert len(mutable_list) == 1


async def test_stop_distributed_long_task(isolate_redis):
    mutable_list = []
    task = DistributedTask(
        name='test_interval_task',
        task_period=60,
        locker_timeout=120,
        task_func=long_task_for_test,
        redis=isolate_redis,
        mutable_list=mutable_list,
    )

    await task.start()
    await asyncio.sleep(0.1)
    await task.stop(0.)

    assert len(mutable_list) == 0


@pytest.mark.parametrize(
    'task_count',
    list(range(1, 11, 3)),
)
async def test_many_distributed_tasks(isolate_redis, task_count):
    mutable_list = []
    tasks = []
    for _ in range(task_count):
        tasks.append(
            DistributedTask(
                name='test_interval_task',
                task_period=0.5,
                locker_timeout=0.6,
                task_func=task_for_test,
                redis=isolate_redis,
                mutable_list=mutable_list,
            )
        )

    for task in tasks:
        await task.start()

    await asyncio.sleep(0.7)
    await asyncio.gather(*[task.stop(0) for task in tasks], return_exceptions=True)

    assert len(mutable_list) == 2


async def test_distributed_task_with_error(isolate_redis):
    called_times = 0

    async def task_that_raises_error_once(mutable_list: list[int]) -> None:
        nonlocal called_times
        called_times += 1
        if called_times == 1:
            raise ValueError('test error')
        mutable_list.append(1)

    mutable_list = []
    task = DistributedTask(
        name='test_interval_task',
        task_period=0.05,
        locker_timeout=0.1,
        task_func=task_that_raises_error_once,
        redis=isolate_redis,
        mutable_list=mutable_list,
    )
    await task.start()
    await asyncio.sleep(0.07)
    await task.stop(0.)

    assert len(mutable_list) == 1


async def test_start_already_running(isolate_redis):
    task = DistributedTask(
        name='test_task',
        task_period=0.05,
        locker_timeout=0.1,
        task_func=task_for_test,
        redis=isolate_redis,
        mutable_list=[],
    )

    await task.start()

    with pytest.raises(RuntimeError):
        await task.start()

    await task.stop(0.)
