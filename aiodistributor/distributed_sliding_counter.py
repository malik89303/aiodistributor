from datetime import datetime
from typing import Any

from redis.asyncio import Redis


class DistributedSlidingCounter:

    LUA_SCRIPT = """
        local key = ARGV[1]
        local current_time = tonumber(ARGV[2])
        local expire_ms = tonumber(ARGV[3])
        redis.call('ZREMRANGEBYSCORE', key, '-inf', current_time - expire_ms / 1000)
        redis.call('ZADD', key, current_time, current_time)
        local amount = redis.call('ZCARD', key)
        redis.call('PEXPIRE', key, expire_ms)
        return amount
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
        self._increaser_and_count_script = self.redis.register_script(self.LUA_SCRIPT)

    async def increase(self) -> int:
        return await self._increaser_and_count_script(
            args=[
                self._key,
                str(datetime.utcnow().timestamp()),
                str(self._lifetime),
            ],
            client=self.redis
        )

    async def reset(self) -> None:
        await self.redis.zremrangebyscore(self._key, '-inf', '+inf')

    async def count(self) -> int:
        return await self.redis.zcard(self._key)
