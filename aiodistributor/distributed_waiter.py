import asyncio
import logging
import math
import uuid
from asyncio import Future, Task
from typing import Any, Coroutine

from redis.asyncio.client import PubSub, Redis
from redis.utils import str_if_bytes

from aiodistributor.common.loggers import distributed_waiter_logger


class DistributedWaiter:
    def __init__(
        self,
        redis: 'Redis[Any]',
        channel_name: str | None = None,
        consumer_timeout: int = 100,  # [sec]
        healthcheck_period: float = 30,  # [sec]
        healthcheck_timeout: float = 30,  # [sec]
        default_timeout: int = 30,  # [sec]
        logger: logging.Logger = distributed_waiter_logger,
    ) -> None:
        self._redis: Redis[Any] = redis
        self._channel_name: str = channel_name or f'waiter_channel:{uuid.uuid4()}'

        self._is_stopped: bool = True
        self._consumer_task: Task[None] | None = None
        self._consumer_timeout: int = consumer_timeout
        self._default_timeout: int = default_timeout

        self._pubsub: PubSub | None = None
        self._futures: dict[str, Future[bool]] = {}

        self._healthcheck_task: Task[None] | None = None
        self._healthcheck_key = f'healthcheck_{self._channel_name}'
        self._healthcheck_period: float = healthcheck_period
        self._healthcheck_timeout: float = healthcheck_timeout

        self._logger = logger

    async def start(self) -> None:
        if not self._is_stopped:
            return

        self._logger.info('starting distributed waiter')
        await self._redis.initialize()
        await self._init_pubsub()
        self._consumer_task = asyncio.create_task(self._run_consumer())
        self._healthcheck_task = asyncio.create_task(self._run_healthcheck())
        self._is_stopped = False

    async def stop(self) -> None:
        if self._is_stopped:
            return

        if self._healthcheck_task is not None:
            self._healthcheck_task.cancel()
            self._healthcheck_task = None

        if self._pubsub is not None:
            await self._pubsub.unsubscribe(self._channel_name)
            self._pubsub = None

        if self._consumer_task is not None:
            self._consumer_task.cancel()
            await asyncio.wait([self._consumer_task])
            self._consumer_task = None

        self._futures = {}
        self._is_stopped = True

    async def _restart(self) -> None:
        self._logger.warning('restarting distributed waiter')
        await self.stop()
        await self.start()

    async def notify(self, key: str) -> bool:
        if self._is_stopped:
            raise ValueError('cannot notify waiter with stopped consumer')

        if (channel := await self._redis.get(key)) is None:
            return False

        if (await self._redis.publish(channel, key)) != 1:
            self._logger.error(f'failed to send message to {channel=}, {key=}')
            return False

        return True

    async def create_waiter(self, key: str, timeout: float | None, expire: int | None) -> Coroutine[Any, Any, Any]:
        if self._is_stopped:
            raise ValueError('cannot create waiter with stopped consumer')

        self._futures[key] = asyncio.Future()
        await self._redis.set(
            name=key,
            value=self._channel_name,
            ex=expire if expire is not None else math.ceil(self._default_timeout),
        )
        return self._wait(self._futures[key], timeout)

    async def _wait(self, future: Future[Any], timeout: float | None) -> bool:
        try:
            return await asyncio.wait_for(
                fut=future,
                timeout=timeout if timeout is not None else self._default_timeout,
            )
        finally:
            del future

    async def _init_pubsub(self) -> None:
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.subscribe(self._channel_name)

    async def _run_consumer(self) -> None:
        if self._pubsub is None:
            raise ValueError('redis pubsub is not initialized')

        while True:
            try:
                response = await self._pubsub.parse_response(
                    block=True,
                    timeout=self._consumer_timeout,
                )
                message = await self._pubsub.handle_message(
                    response=response,
                    ignore_subscribe_messages=True,
                )
                if message is None:
                    continue

                if (message := str_if_bytes(message['data'])) != self._healthcheck_key:
                    self._logger.info(f'got {message=} from redis {self._channel_name=}')
                else:
                    self._logger.debug(f'got {message=} from redis {self._channel_name=}')

                if (future := self._futures.get(message)) is not None:
                    future.set_result(True)
                elif message == self._healthcheck_key:
                    self._logger.error('got healthcheck message, but healthcheck future not found')
                    asyncio.create_task(self._restart())
                    return
                else:
                    self._logger.error(f'future not found for {message=}')

            except asyncio.CancelledError:
                self._logger.warning('consumer task cancelled')
                raise
            except BaseException:
                self._logger.exception(f'exception occurred while reading message from {self._channel_name=}')
                await asyncio.sleep(self._healthcheck_period)

    async def _run_healthcheck(self) -> None:
        while True:
            await asyncio.sleep(self._healthcheck_period)
            self._logger.debug('starting healthcheck')

            try:
                await self._healthcheck()
            except asyncio.CancelledError:
                self._logger.warning('healthcheck task cancelled')
                raise
            except BaseException:
                self._logger.exception('failed to healthcheck')
                asyncio.create_task(self._restart())

    async def _healthcheck(self) -> None:
        if self._pubsub is None:
            raise ValueError('redis pubsub is not initialized')
        elif self._pubsub.connection is None:
            raise ValueError('empty pubsub connection')

        await self._pubsub.ping(self._healthcheck_key)

        self._futures[self._healthcheck_key] = asyncio.Future()
        await self._wait(self._futures[self._healthcheck_key], self._healthcheck_timeout)
