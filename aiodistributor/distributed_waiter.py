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
    """
    DistributedWaiter class is an asyncio-based implementation of a distributed waiter,
    which waits for signals from other nodes or services and triggers the appropriate callback.

    The class uses Redis to implement a publish-subscribe model for communication between nodes.
    It can subscribe to a Redis channel and receive messages, and it can also publish messages to a channel.

    The waiter can create a wait for a specified key and wait for a signal to be received before continuing.
    The wait can also have a timeout, after which the waiter will resume execution.

    The class can also be started and stopped.
     When started, it initializes the Redis connection and starts the poller and healthcheck tasks.
    When stopped, it cancels the tasks and cleans up any state.

    """

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
        """
        :param redis: The Redis client object used to connect to the Redis server.
        :param channel_name:The name of the Redis channel that the poller is subscribed to.
        :param consumer_timeout: The amount of time in seconds that the consumer will wait for a message before
         timing out.
        :param healthcheck_period:The amount of time in seconds between healthcheck messages.
        :param healthcheck_timeout: The amount of time in seconds that the waiter will wait for a healthcheck message
         before restarting.
        :param default_timeout: The default amount of time in seconds that a waiter will wait for a signal before
         timing out.
        :param logger:The logger object used to log events and errors.
        """
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
        """
        Starts the distributed waiter.
        """
        if not self._is_stopped:
            return

        self._logger.info('starting distributed waiter')
        await self._redis.initialize()
        await self._init_pubsub()
        self._consumer_task = asyncio.create_task(self._run_consumer())
        self._healthcheck_task = asyncio.create_task(self._run_healthcheck())
        self._is_stopped = False

    async def stop(self) -> None:
        """
        Stops the distributed waiter.
        """
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
        """
        Restarts the distributed waiter.
        """
        self._logger.warning('restarting distributed waiter')
        await self.stop()
        await self.start()

    async def notify(self, key: str) -> bool:
        """
        Notifies the waiter that a signal has been received for the specified key.

        :param key:  key for the signal.
        :return: True if the signal was successfully sent; otherwise, False.
        """
        if self._is_stopped:
            raise ValueError('cannot notify waiter with stopped consumer')

        if (channel := await self._redis.get(key)) is None:
            return False

        if (await self._redis.publish(channel, key)) != 1:
            self._logger.error(f'failed to send message to {channel=}, {key=}')
            return False

        return True

    async def create_waiter(self, key: str, timeout: float | None, expire: int | None) -> Coroutine[Any, Any, Any]:
        """
        Creates a new waiter for the specified key.

        :param key:The key for the signal.
        :param timeout:The amount of time in seconds that the waiter will wait for a signal before timing out.
         Defaults to None.
        :param expire: The amount of time in seconds before the waiter's key expires. Defaults to None.

        :return: The coroutine for waiting for the signal.
        """
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
        """
        Waits for the specified future to complete.
        :param future: The future to wait for.
        :param timeout: The amount of time in seconds that the waiter will wait for the future before timing out.
         Defaults to None.

        :return:True if signal received successfully; otherwise, False.
        """
        try:
            return await asyncio.wait_for(
                fut=future,
                timeout=timeout if timeout is not None else self._default_timeout,
            )
        finally:
            del future

    async def _init_pubsub(self) -> None:
        """
        Initializes the Redis pubsub object.
        """
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.subscribe(self._channel_name)

    async def _run_consumer(self) -> None:
        """
        Runs the consumer task that listens for messages from Redis.
        """
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
        """
        Runs the healthcheck task that periodically sends healthcheck messages to Redis.
        """
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
        """
        Sends a healthcheck message to Redis.
        """
        if self._pubsub is None:
            raise ValueError('redis pubsub is not initialized')
        elif self._pubsub.connection is None:
            raise ValueError('empty pubsub connection')

        await self._pubsub.ping(self._healthcheck_key)

        self._futures[self._healthcheck_key] = asyncio.Future()
        await self._wait(self._futures[self._healthcheck_key], self._healthcheck_timeout)
