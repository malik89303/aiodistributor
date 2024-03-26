from typing import Any

from redis.asyncio import Redis


class DistributedEvent:
    def __init__(self, redis: 'Redis[Any]', name: str, consumer_timeout: float = 5):
        self._redis = redis
        self._name = f'event_{name}'
        self._channel_name = f'{self._name}_channel'
        self._pubsub = self._redis.pubsub()
        self._consumer_timeout = consumer_timeout

    async def is_set(self) -> bool:
        """Return True if and only if the internal flag is true."""
        return await self._redis.get(self._name) == '1'

    async def set(self) -> None:
        """Set the internal flag to true. Notify all coroutines waiting for it."""
        await self._redis.set(self._name, '1')
        await self._redis.publish(self._channel_name, '1')

    async def clear(self) -> None:
        """Reset the internal flag to false."""
        await self._redis.set(self._name, '0')

    async def wait(self) -> bool:
        """Block until the internal flag is true."""
        if await self.is_set():
            return True

        await self._pubsub.subscribe(self._channel_name)
        try:
            while True:
                message = await self._pubsub.get_message(
                    timeout=self._consumer_timeout,
                    ignore_subscribe_messages=True,
                )
                if message and message['data'] == '1':
                    return True
                if await self.is_set():  # Check again to avoid race condition
                    return True
        finally:
            await self._pubsub.unsubscribe(self._channel_name)
