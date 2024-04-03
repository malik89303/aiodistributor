from typing import Any

from redis.asyncio import Redis

from aiodistributor.distributed_lock import DistributedLock


class BrokenBarrierError(ValueError):
    pass


class DistributedBarrier:
    def __init__(self, redis: 'Redis[Any]', key: str, parties: int):
        if parties < 1:
            raise ValueError('parties must be > 0')
        self._redis = redis
        self._key = key
        self._parties = parties
        self._count_key = f'distributed_barrier_count_{key}'
        self._channel_name = f'distributed_barrier_channel{key}'
        self._lock = DistributedLock(redis, f'barrier_{key}')

    async def wait(self) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(self._channel_name)

        async with self._lock:
            count = await self._redis.incr(self._count_key)
            if count >= self._parties:
                await self._reset_count()
                await self._publish_release()

        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10.0)
                if message:
                    if message['data'] == 'release':
                        break
                    elif message['data'] in ('reset', 'abort'):
                        raise BrokenBarrierError('Barrier has been reset or aborted.')
        finally:
            await pubsub.unsubscribe(self._channel_name)

    async def _reset_count(self) -> None:
        await self._redis.set(self._count_key, 0)

    async def _publish_release(self) -> None:
        for _ in range(self._parties):
            await self._redis.publish(self._channel_name, 'release')

    async def reset(self) -> None:
        async with self._lock:
            await self._reset_count()
            await self._redis.publish(self._channel_name, 'reset')

    async def abort(self) -> None:
        async with self._lock:
            await self._reset_count()
            await self._redis.publish(self._channel_name, 'abort')
