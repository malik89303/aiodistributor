from datetime import datetime
from typing import Any

from redis.asyncio import Redis


class DistributedSlidingCounter:
    """
    This class is a Python implementation of a distributed sliding counter that utilizes Redis for storage.
    The sliding counter counts events that occur within a specific time window and can be used to implement
     rate limiting or throttle mechanisms.
    """

    def __init__(
        self,
        redis: 'Redis[Any]',
        key: str,
        lifetime: int = 10 * 1000,  # [msec]
    ) -> None:
        self.redis: Redis[Any] = redis
        self._key: str = key
        self._lifetime: int = lifetime

    async def increase(self) -> int:
        """
        increases sliding counter
        :return: current counter value
        """
        now = datetime.utcnow().timestamp()
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(self._key, '-inf', now - self._lifetime / 1000)
        pipe.zadd(self._key, {now: now})  # type: ignore
        pipe.zcard(self._key)
        pipe.pexpire(self._key, self._lifetime)
        _, _, count, _ = await pipe.execute()

        return count

    async def reset(self) -> int:
        """
        resets counter to zero
        :return: current counter value
        """
        await self.redis.zremrangebyscore(self._key, '-inf', '+inf')
        return await self.count()

    async def count(self) -> int:
        """
        :return: returns current counter value
        """
        return await self.redis.zcard(self._key)
