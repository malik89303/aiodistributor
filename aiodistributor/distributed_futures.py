import asyncio
import logging
import uuid
from asyncio import AbstractEventLoop, Future, Task
from typing import Coroutine

from redis.asyncio.client import PubSub, Redis
from redis.utils import str_if_bytes


class RedisCondition:
    def __init__(
        self,
        redis: Redis,
        loop: AbstractEventLoop | None = None,
        channel_name: str = f'channel:{uuid.uuid4()}',
        consumer_timeout: int = 10,  # [sec]
        healthcheck_timeout: float = 30,  # [sec]
        # logger: Logger # TOOD logging
    ) -> None:
        self._redis: Redis = redis
        self._channel_name: str = channel_name
        self._loop: AbstractEventLoop = loop or asyncio.new_event_loop()

        self._is_stopped: bool = True
        self._consumer_task: Task[None] | None = None
        self._consumer_timeout: int = consumer_timeout

        self._pubsub: PubSub | None = None
        self._futures: dict[str, Future[bool]] = {}

        self._healthcheck_task: Task[None] | None = None
        self._healthcheck_key = f'healthcheck_{self._channel_name}'
        self._healthcheck_delay: float = healthcheck_timeout

        self._logger = logging.getLogger('aiodistributor_futures')  # TODO принимать как-то

    async def start(self) -> None:
        if not self._is_stopped:
            raise ValueError('redis consumer already started')

        self._logger.info('starting redis condition')
        await self._redis.initialize()
        await self._init_pubsub()
        self._consumer_task = asyncio.create_task(self._run_consumer())
        self._healthcheck_task = asyncio.create_task(self._run_healthcheck())
        self._is_stopped = False

    async def stop(self) -> None:
        if self._is_stopped:
            raise ValueError('redis consumer already stopped')

        self._healthcheck_task.cancel()
        self._healthcheck_task = None
        await self._pubsub.unsubscribe(self._channel_name)
        self._pubsub = None
        self._consumer_task.cancel()
        await asyncio.wait([self._consumer_task])
        self._consumer_task = None
        self._futures = {}

        self._is_stopped = True

    async def restart(self) -> None:
        self._logger.warning('restarting redis condition')
        await self.stop()
        await self.start()

    async def notify(self, var: str) -> bool:
        if (channel := await self._redis.get(var)) is None:
            return False

        if (await self._redis.publish(channel, var)) != 1:
            self._logger.error(f'failed to send message to {channel=}, {var=}')
            return False

        return True

    async def create_waiter(self, var: str, expire: int = 300, timeout: int = 30) -> Coroutine:
        self._futures[var] = self._loop.create_future()
        await self._redis.set(var, self._channel_name, ex=expire)
        return self._wait(self._futures[var], timeout)

    async def _wait(self, future: Future, timeout: float = 30) -> bool:
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            del future

    async def _init_pubsub(self) -> None:
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.subscribe(self._channel_name)

    async def _run_consumer(self) -> None:
        if self._pubsub is None:
            raise ValueError('redis consumer pubsub is not initialized')

        while True:
            try:
                response = await self._pubsub.parse_response(
                    block=True,
                    timeout=self._consumer_timeout,
                )
                message = await self._pubsub.handle_message(
                    response,
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
                else:
                    if message == self._healthcheck_key:
                        self._logger.error('got healthcheck message, but healthcheck future not found')
                        asyncio.create_task(self.restart())
                    self._logger.error(f'future not found for {message=}')

            except asyncio.CancelledError:
                self._logger.warning('redis consumer task cancelled')
                raise
            except Exception:
                self._logger.exception(f'exception occurred while reading message from {self._channel_name=}')
                await asyncio.sleep(3)

    async def _run_healthcheck(self) -> None:
        while True:
            await asyncio.sleep(self._healthcheck_delay)
            self._logger.debug('starting condition healthcheck')

            try:
                is_alive = await self._healthcheck()
            except asyncio.CancelledError:
                self._logger.warning('redis condition healthcheck task cancelled')
                raise
            except Exception:
                self._logger.exception('failed to healthcheck redis condition')
                is_alive = False

            if not is_alive:
                asyncio.create_task(self.restart())
                return

    async def _healthcheck(self) -> bool:
        if self._pubsub is None:
            raise ValueError('redis consumer pubsub is not initialized')

        if self._pubsub.connection is None:
            self._logger.error('empty connection for pubsub')
            return False

        self._futures[self._healthcheck_key] = self._loop.create_future()
        try:
            await self._pubsub.ping(self._healthcheck_key)
        except ConnectionError:
            self._logger.exception('connection error occurred while pinging condition')
            return False

        try:
            healthcheck_result = await asyncio.wait_for(
                fut=self._futures[self._healthcheck_key],
                timeout=1,
            )
        except asyncio.TimeoutError:
            self._logger.exception('healthcheck ping result was not received')
            healthcheck_result = False

        del self._futures[self._healthcheck_key]
        return healthcheck_result
