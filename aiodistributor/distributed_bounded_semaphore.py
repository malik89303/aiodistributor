from typing import Any

from redis.asyncio import Redis

from aiodistributor.distibuted_semaphore import DistributedSemaphore


class DistributedBoundedSemaphore(DistributedSemaphore):
    LUA_RELEASE_SCRIPT = """
    local current_count = redis.call('get', KEYS[1]) or 0
    local max_value = redis.call('get', KEYS[2]) or 0
    if tonumber(current_count) < tonumber(max_value) then
        redis.call('SET', KEYS[1], current_count + 1)
        return 1
    else
        return 0
    end
    """

    def __init__(
        self,
        redis: 'Redis[Any]',
        key: str,
        value: int,
        acquire_sleep_delay: float = 0.1,
        acquire_timeout: float | None = None,
    ) -> None:
        """
        Initializes a distributed bounded semaphore using Redis.

        :param redis: Redis client to use for the semaphore.
        :param key: The key used in Redis to store the semaphore state.
        :param value: The maximum count for the semaphore.
        :param acquire_sleep_delay: Delay in seconds between attempts to acquire the semaphore.
        :param acquire_timeout: Maximum time in seconds to spend attempting to acquire the semaphore.
        """
        super().__init__(redis, key, value, acquire_sleep_delay, acquire_timeout)
        self._max_value = value
        self._initialized = False
        self._max_value_key = f'{self._key}_max_value'

    async def _initialize_max_value(self) -> None:
        await self._redis.setnx(self._max_value_key, self._value)

    async def release(self) -> None:
        """
        Asynchronously releases the distributed bounded semaphore.

        :raises ValueError: If the semaphore's value would be increased above the initial value.
        """
        if not self._initialized:
            await self._initialize_max_value()
            self._initialized = True

        released = await self._release_script(keys=[self._key, self._max_value_key], client=self._redis)
        if not released:
            raise ValueError('DistributedBoundedSemaphore released too many times')
