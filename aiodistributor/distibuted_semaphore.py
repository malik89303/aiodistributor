import asyncio
from typing import Any

from redis.asyncio import Redis


class DistributedSemaphore:
    LUA_ACQUIRE_SCRIPT = """
    local current_count = redis.call('get', KEYS[1]) or ARGV[1]
    if tonumber(current_count) > 0 then
        redis.call('SET', KEYS[1], current_count - 1)
        return 1
    else
        return 0
    end
    """

    LUA_RELEASE_SCRIPT = """
    local current_count = redis.call('get', KEYS[1]) or 0
    redis.call('SET', KEYS[1], current_count + 1)
    return 1
    """

    def __init__(
        self,
        redis: 'Redis[Any]',
        key: str,
        value: int,
        acquire_sleep_delay: float = 0.1,
        acquire_timeout: float | None = None
    ) -> None:
        """
        Initializes a distributed semaphore using Redis.

        :param redis: Redis client to use for the semaphore.
        :param key: The key used in Redis to store the semaphore state.
        :param value: The maximum count for the semaphore.
        :param acquire_sleep_delay: Delay in seconds between attempts to acquire the semaphore.
        :param acquire_timeout: Maximum time in seconds to spend attempting to acquire the semaphore.
        """
        if value < 0:
            raise ValueError('Semaphore initial value must be >= 0')

        self._redis = redis
        self._key = f'distributed_semaphore_{key}'
        self._value = value
        self._acquire_sleep_delay = acquire_sleep_delay
        self._acquire_timeout = acquire_timeout

        self._acquire_script = redis.register_script(self.LUA_ACQUIRE_SCRIPT)
        self._release_script = redis.register_script(self.LUA_RELEASE_SCRIPT)

    async def acquire(self, acquire_sleep_delay: float | None = None, acquire_timeout: float | None = None) -> bool:
        """
        Asynchronously tries to acquire the distributed semaphore.

        :param acquire_sleep_delay: Override the default sleep delay between acquisition attempts.
        :param acquire_timeout: Override the default timeout for acquiring the semaphore.

        :return: True if the semaphore was successfully acquired.

        :raises asyncio.TimeoutError: If the semaphore cannot be acquired within the specified timeout.
        """
        if acquire_sleep_delay is None:
            acquire_sleep_delay = self._acquire_sleep_delay
        if acquire_timeout is None:
            acquire_timeout = self._acquire_timeout

        acquired = False
        try:
            async with asyncio.timeout(acquire_timeout):
                while not acquired:
                    acquired = await self._acquire_script(keys=[self._key], args=[self._value], client=self._redis)
                    if not acquired:
                        await asyncio.sleep(acquire_sleep_delay)
                return True
        except asyncio.TimeoutError:
            if acquired:
                await self._release_script(keys=[self._key], args=[], client=self._redis)
            raise

    async def release(self) -> None:
        """
        Asynchronously releases the distributed semaphore.

        :raises RuntimeError: If the semaphore cannot be released.
        """
        await self._release_script(keys=[self._key], args=[], client=self._redis)

    async def __aenter__(self) -> 'DistributedSemaphore':
        """
        Asynchronously enters the runtime context related to this object.

        :return: The instance of the semaphore itself.
        """
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: type, exc: BaseException, tb: type) -> None:
        """
        Asynchronously exits the runtime context related to this object.

        :param exc_type: The exception type raised (if any).
        :param exc: The exception instance raised (if any).
        :param tb: The traceback object (if any).
        """
        await self.release()

    async def locked(self) -> bool:
        """
        Checks if the semaphore is currently locked in Redis.

        :return: True if the semaphore is locked (no available slots), False otherwise.
        """
        current_count = await self._redis.get(self._key) or self._value
        return int(current_count) == 0
