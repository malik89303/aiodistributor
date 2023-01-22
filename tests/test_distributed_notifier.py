import asyncio
from datetime import datetime, timedelta

import pytest

from aiodistributor.distributed_notifier import DistributedNotifier

STREAM_ID = 'aboba'


async def test_add_message_to_stream(isolate_distributed_notifier: DistributedNotifier):
    first_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID,
        message='boba'
    )
    assert first_message_id is not None

    second_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID,
        message='a'
    )
    assert second_message_id is not None

    [
        [first_stream_id, [(selected_first_message_id, first_message), (selected_second_message_id, second_message)]]
    ] = await isolate_distributed_notifier._redis.xread(
        streams={STREAM_ID: '0'}
    )

    assert first_stream_id == STREAM_ID
    assert first_message == {'data': 'boba'}
    assert second_message == {'data': 'a'}
    assert selected_first_message_id == first_message_id
    assert selected_second_message_id == second_message_id


async def test_get_message_from_stream_empty(isolate_distributed_notifier: DistributedNotifier):
    res = await isolate_distributed_notifier.wait_message_from_stream(
        stream_id=STREAM_ID,
        last_message_id='0',
        block_timeout=None,
    )
    assert res == (None, None)


async def test_get_message_from_stream_single_not_blocking(isolate_distributed_notifier: DistributedNotifier):
    message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(stream_id=STREAM_ID, message='boba')

    selected_message_id, selected_message = await isolate_distributed_notifier.wait_message_from_stream(
        stream_id=STREAM_ID,
        last_message_id='0',
        block_timeout=None,
    )
    assert selected_message_id == message_id
    assert selected_message == 'boba'


async def test_get_message_from_stream_single_blocking(isolate_distributed_notifier: DistributedNotifier):
    task = asyncio.create_task(
        isolate_distributed_notifier.wait_message_from_stream(
            stream_id=STREAM_ID,
            last_message_id='0',
            block_timeout=1,
        )
    )
    await asyncio.sleep(0.01)
    message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(stream_id=STREAM_ID, message='boba')

    selected_message_id, selected_message = await task
    assert selected_message_id == message_id
    assert selected_message == 'boba'


async def test_get_message_from_stream_multiple(isolate_distributed_notifier: DistributedNotifier):
    first_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='boba'
    )
    second_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='a'
    )

    selected_message_id, selected_message = await isolate_distributed_notifier.wait_message_from_stream(
        stream_id=STREAM_ID,
        last_message_id='0',
        block_timeout=None,
    )
    assert selected_message_id == first_message_id
    assert selected_message == 'boba'

    selected_message_id, selected_message = await isolate_distributed_notifier.wait_message_from_stream(
        stream_id=STREAM_ID,
        last_message_id=first_message_id,
        block_timeout=None,
    )
    assert selected_message_id == second_message_id
    assert selected_message == 'a'


async def test_get_last_stream_message_id_default(isolate_distributed_notifier: DistributedNotifier):
    res = await isolate_distributed_notifier.get_last_stream_message(STREAM_ID)
    assert res == (None, None)


async def test_get_last_stream_message_id(isolate_distributed_notifier: DistributedNotifier):
    first_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='boba'
    )
    msg_id, msg = await isolate_distributed_notifier.get_last_stream_message(STREAM_ID, offset=1)
    assert msg_id == first_message_id
    assert msg == 'boba'

    second_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='a'
    )

    res = await isolate_distributed_notifier.get_last_stream_message(STREAM_ID)
    assert res[0] == second_message_id

    third_message_id = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='mogus'
    )
    res = await isolate_distributed_notifier.get_last_stream_message(STREAM_ID)
    assert res[0] == third_message_id

    res = await isolate_distributed_notifier.get_last_stream_message(STREAM_ID, offset=2)
    assert res[0] == second_message_id
    res = await isolate_distributed_notifier.get_last_stream_message(STREAM_ID, offset=3)
    assert res[0] == first_message_id


async def test_add_message_to_stream_lower_timestamp(isolate_distributed_notifier: DistributedNotifier):
    now = datetime.utcnow()

    res = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='boba', timestamp=now
    )
    assert res is not None

    res = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID, message='boba', timestamp=now
    )
    assert res is not None

    res = await isolate_distributed_notifier.add_message_to_stream_if_newer(
        stream_id=STREAM_ID,
        message='boba',
        timestamp=now - timedelta(minutes=10),
    )
    assert res is None


async def test_get_message_empty(isolate_distributed_notifier: DistributedNotifier):
    messages = []

    async def background_consumer():
        async for message in isolate_distributed_notifier.get_message(stream_id='aboba'):
            messages.append(message)

    task = asyncio.create_task(background_consumer())

    for i in range(20):
        await isolate_distributed_notifier.add_message_to_stream_if_newer(
            stream_id='aboba',
            message=str(i),
            timestamp=None,
        )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert messages == list(map(str, range(20)))


async def test_get_message_filled(isolate_distributed_notifier: DistributedNotifier):
    messages = []

    async def background_consumer():
        async for message in isolate_distributed_notifier.get_message(stream_id='aboba'):
            messages.append(message)

    for i in range(5):
        await isolate_distributed_notifier.add_message_to_stream_if_newer(
            stream_id='aboba',
            message=str(i),
            timestamp=None,
            max_len=None,
        )

    task = asyncio.create_task(background_consumer())
    await asyncio.sleep(0.01)

    for i in range(5, 7):
        await isolate_distributed_notifier.add_message_to_stream_if_newer(
            stream_id='aboba',
            message=str(i),
            timestamp=None,
            max_len=None,
        )

    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert messages == ['4', '5', '6']
