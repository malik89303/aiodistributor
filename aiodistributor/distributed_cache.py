import functools
from typing import Any, Callable

import ujson
from redis.asyncio.client import Redis


def distributed_cache(
    redis: 'Redis[Any]',
    expire: int | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Asynchronous decorator to cache the result of a function in Redis.

    :param redis: Redis client to use for caching.
    :param expire: Number of miliseconds for the cache to expire.

    :return: Decorator that caches function results in Redis.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = f'{func.__name__}:{ujson.dumps((args, kwargs), sort_keys=True)}'
            result = await redis.get(key)

            if result:
                return ujson.loads(result)
            else:
                result = await func(*args, **kwargs)
                await redis.set(
                    name=key,
                    value=ujson.dumps(result),
                    px=expire,
                )
                return result

        return wrapper

    return decorator
