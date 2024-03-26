from typing import Any

from redis.asyncio import Redis

from aiodistributor.distributed_lock import DistributedLock


class DistributedCondition:
    LUA_DECR_SCRIPT = """
        local counter_key = KEYS[1]
        local counter = redis.call('DECR', counter_key)
        if counter >= 0 then
            return 1
        else
            redis.call('INCR', counter_key)  -- Restore the counter if no notification is required.
            return 0
        end
        """

    def __init__(
        self,
        redis: 'Redis[Any]',
        key: str,
        acquire_sleep_delay: float = 0.1,
        acquire_timeout: float | None = None,
    ) -> None:
        self._redis = redis
        self._lock = DistributedLock(
            redis=redis,
            key=key,
            acquire_sleep_delay=acquire_sleep_delay,
            acquire_timeout=acquire_timeout,
        )
        self._channel_name = f'condition_{key}_channel'
        self._notify_counter_key = f'{self._channel_name}_notify_counter'
        self._pubsub = self._redis.pubsub()
        self._decr_script = self._redis.register_script(self.LUA_DECR_SCRIPT)

    async def wait(self) -> bool:
        await self._pubsub.subscribe(self._channel_name)
        try:
            while True:
                message = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=10.0)
                if message and message['data'] == '1':
                    result = await self._decr_script(keys=[self._notify_counter_key], args=[], client=self._redis)
                    if result == 1:
                        return True
        finally:
            await self._pubsub.unsubscribe(self._channel_name)

    async def notify(self, n: int = 1) -> None:
        if not await self._lock.locked():
            raise RuntimeError('Cannot notify on a condition without holding the lock')
        await self._redis.incrby(self._notify_counter_key, n)
        await self._redis.publish(self._channel_name, '1')

    async def notify_all(self) -> None:
        if not await self._lock.locked():
            raise RuntimeError('Cannot notify on a condition without holding the lock')

        num_subscribers = (await self._redis.pubsub_numsub(self._channel_name))[0][1]
        await self.notify(n=num_subscribers)

    async def __aenter__(self) -> 'DistributedCondition':
        await self._lock.__aenter__()
        return self

    async def __aexit__(self, exc_type: type, exc: BaseException, tb: type) -> None:
        await self._lock.__aexit__(exc_type, exc, tb)
