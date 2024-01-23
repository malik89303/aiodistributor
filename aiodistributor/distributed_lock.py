import asyncio
import uuid
from typing import Any

from redis.asyncio import Redis


class DistributedLock:
    LUA_RELEASE_SCRIPT = """
        local lock_token = redis.call('get', KEYS[1])
        if lock_token == ARGV[1] then
            redis.call('del', KEYS[1])
            return 1
        end
        return 0
        """

    def __init__(
        self,
        redis: 'Redis[Any]',
        key: str,
        acquire_sleep_delay: float = 0.1,
        acquire_timeout: float | None = None,
    ) -> None:
        """
        Initializes a distributed lock using Redis.

        :param redis: Redis client to use for the lock.
        :param key: The key used in Redis to store the lock state.
        :param acquire_sleep_delay: Delay in seconds between attempts to acquire the lock.
        :param acquire_timeout: Maximum time in seconds to spend attempting to acquire the lock.
        """
        self._redis = redis
        self._key = f'distributed_lock_{key}'
        self._token = str(uuid.uuid4())
        self._acquire_sleep_delay = acquire_sleep_delay
        self._acquire_timeout = acquire_timeout
        self._release_script = redis.register_script(self.LUA_RELEASE_SCRIPT)

    async def acquire(self, acquire_sleep_delay: float | None = None, acquire_timeout: float | None = None) -> bool:
        """
        Asynchronously tries to acquire the distributed lock.

        :param acquire_sleep_delay: Override the default sleep delay between acquisition attempts.
        :param acquire_timeout: Override the default timeout for acquiring the lock.

        :return: True if the lock was successfully acquired.

        :raises asyncio.TimeoutError: If the lock cannot be acquired within the specified timeout.
        """
        if acquire_sleep_delay is None:
            acquire_sleep_delay = self._acquire_sleep_delay
        if acquire_timeout is None:
            acquire_timeout = self._acquire_timeout

        try:
            async with asyncio.timeout(acquire_timeout):
                while not await self._redis.set(self._key, self._token, nx=True):
                    await asyncio.sleep(acquire_sleep_delay)
                return True
        except asyncio.TimeoutError:
            await self._try_release()
            raise

    async def _try_release(self) -> bool:
        """
        Attempts to release the lock if it is held by this instance.

        :return: True if the lock was released, False otherwise.
        """
        return bool(await self._release_script(keys=[self._key], args=[self._token], client=self._redis))

    async def release(self) -> None:
        """
        Asynchronously releases the distributed lock.

        :raises RuntimeError: If the lock is not acquired or owned by this instance.
        """
        if not await self._try_release():
            raise RuntimeError("Cannot release a lock that's not acquired or owned by this instance.")

    async def __aenter__(self) -> 'DistributedLock':
        """
        Asynchronously enters the runtime context related to this object.

        :return: The instance of the lock itself.
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
        Checks if the lock is currently acquired in Redis.

        :return: True if the lock is acquired, False otherwise.
        """
        return await self._redis.exists(self._key) == 1
