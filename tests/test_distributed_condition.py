import asyncio

import pytest

from aiodistributor.distributed_condition import DistributedCondition


async def test_wait_with_notification(isolate_redis):
    condition = DistributedCondition(isolate_redis, 'test_condition')

    async def notifier():
        await asyncio.sleep(0.1)
        async with condition:
            await condition.notify()

    notify_task = asyncio.create_task(notifier())

    await condition.wait()
    await notify_task


async def test_wait_without_lock_raises_error(isolate_redis):
    condition = DistributedCondition(isolate_redis, 'test_condition')

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(condition.wait(), timeout=0.1)


async def test_notify_publishes_message(isolate_redis):
    condition = DistributedCondition(isolate_redis, 'test_condition')

    async def poll_message():
        pubsub = isolate_redis.pubsub()
        await pubsub.subscribe(condition._channel_name)
        while True:
            message = await pubsub.get_message(
                timeout=1,
                ignore_subscribe_messages=True,
            )
            if message:
                await pubsub.unsubscribe(condition._channel_name)
                return message

    message_getter = asyncio.create_task(poll_message())
    await asyncio.sleep(0.01)
    async with condition:
        await condition.notify()

    message = await message_getter
    assert message is not None
    assert message['data'] == '1'


async def test_notify_all_empty(isolate_redis):
    condition = DistributedCondition(isolate_redis, 'test_condition')
    async with condition:
        await condition.notify_all()


async def test_notify_all_with_multiple_waiters(isolate_redis):
    condition = DistributedCondition(isolate_redis, 'test_condition')

    async def waiter(identifier):
        cond = DistributedCondition(isolate_redis, 'test_condition')
        await cond.wait()

    waiter_tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.1)

    async with condition:
        await condition.notify_all()

    await asyncio.wait_for(asyncio.gather(*waiter_tasks), timeout=0.1)


async def test_wait_timeout(isolate_redis):
    condition = DistributedCondition(isolate_redis, 'test_condition')

    with pytest.raises(TimeoutError):
        async with condition:
            await asyncio.wait_for(condition.wait(), timeout=0.1)


async def test_notify_multiple(isolate_redis):
    async def waiter(r):
        cond = DistributedCondition(isolate_redis, 'test_condition')
        await cond.wait()
        r.append(True)

    results = []
    waiter_tasks = [asyncio.create_task(waiter(results)) for _ in range(6)]
    await asyncio.sleep(0.1)

    condition = DistributedCondition(isolate_redis, 'test_condition')
    async with condition:
        await condition.notify(4)
        await asyncio.sleep(0.1)
        assert len(results) == 4

        await condition.notify(2)
        await asyncio.sleep(0.1)
        assert len(results) == 6

    await asyncio.gather(*waiter_tasks)
