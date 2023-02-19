import asyncio

from aiodistributor.distributed_cache import distributed_cache


async def test_distributed_cache(isolate_redis):
    called_times = 0

    @distributed_cache(redis=isolate_redis, expire=100)
    async def slow_function(n):
        nonlocal called_times
        await asyncio.sleep(0.1)
        called_times += 1
        return n * called_times

    # First call should take a second and cache the result
    result = await slow_function(42)
    assert result == 42

    cached_result = await isolate_redis.get('slow_function:[[42],{}]')
    assert cached_result is not None
    assert cached_result == '42'

    # Second call should return the cached result immediately
    result = await slow_function(42)
    assert result == 42

    # Wait for the cache to expire
    await asyncio.sleep(0.3)

    cached_result = await isolate_redis.get('slow_function:[[42],{}]')
    assert cached_result is None

    # Third call should take a second again and cache the result
    result = await slow_function(42)
    assert result == 84
