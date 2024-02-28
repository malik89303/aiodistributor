import asyncio

import pytest

from aiodistributor.distributed_event import DistributedEvent


async def test_event_set_and_clear(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event')
    assert not await event.is_set()

    await event.set()
    assert await event.is_set()

    await event.clear()
    assert not await event.is_set()


async def test_event_wait_for_set(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event')

    async def setter():
        await asyncio.sleep(0.1)  # Ждём перед установкой события
        await event.set()

    asyncio.create_task(setter())
    await event.wait()  # Должно вернуть True после установки события
    assert await event.is_set()


async def test_event_wait_timeout(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event')

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(event.wait(), timeout=0.2)


async def test_multiple_instances_waiting_for_event_set(isolate_redis):
    event1 = DistributedEvent(isolate_redis, 'test_event')
    event2 = DistributedEvent(isolate_redis, 'test_event')

    async def set_event():
        await asyncio.sleep(0.1)  # Даем время на инициализацию слушателей
        await event1.set()  # Установка события через первый инстанс

    set_task = asyncio.create_task(set_event())
    wait_task1 = asyncio.create_task(event1.wait())
    wait_task2 = asyncio.create_task(event2.wait())

    await asyncio.gather(set_task, wait_task1, wait_task2)

    assert wait_task1.result() is True
    assert wait_task2.result() is True
    assert await event1.is_set()
    assert await event2.is_set()


async def test_event_reset_and_wait_again(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event')

    await event.set()
    assert await event.is_set()

    await event.clear()
    assert not await event.is_set()

    async def set_event_again():
        await asyncio.sleep(0.1)
        await event.set()

    set_task = asyncio.create_task(set_event_again())
    wait_task = asyncio.create_task(event.wait())

    await asyncio.gather(set_task, wait_task)

    assert wait_task.result() is True
    assert await event.is_set()


async def test_wait_with_timeout_multiple_instances(isolate_redis):
    event1 = DistributedEvent(isolate_redis, 'test_event')
    event2 = DistributedEvent(isolate_redis, 'test_event')

    async def wait_with_timeout(event):
        try:
            await asyncio.wait_for(event.wait(), timeout=0.1)
            return True
        except asyncio.TimeoutError:
            return False

    wait_task1 = asyncio.create_task(wait_with_timeout(event1))
    wait_task2 = asyncio.create_task(wait_with_timeout(event2))

    results = await asyncio.gather(wait_task1, wait_task2, return_exceptions=True)

    assert all(result is False for result in results)  # Оба задания должны завершиться по тайм-ауту
    assert not await event1.is_set()
    assert not await event2.is_set()


async def test_event_is_already_set_before_wait(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event')
    await event.set()  # Устанавливаем событие перед ожиданием

    # Проверяем, что wait немедленно возвращает True, если событие уже установлено
    assert await event.wait() is True


async def test_event_is_set_during_wait(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event', 1)

    wait_task = asyncio.create_task(event.wait())
    await asyncio.sleep(0.1)
    await event._redis.set(event._name, '1')

    # Проверяем, что wait возвращает True, когда событие устанавливается во время его ожидания
    assert await wait_task is True


async def test_event_set_and_clear_with_redis_check(isolate_redis):
    event = DistributedEvent(isolate_redis, 'test_event')

    assert not await event.is_set()
    assert await isolate_redis.get(event._name) is None

    await event.set()
    assert await event.is_set()
    assert await isolate_redis.get(event._name) == '1'

    await event.clear()
    assert not await event.is_set()
    assert await isolate_redis.get(event._name) == '0'
