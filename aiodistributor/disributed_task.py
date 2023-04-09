import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from redis.asyncio import Redis
from redis.asyncio.lock import Lock
from redis.exceptions import LockError

from aiodistributor.common.loggers import distributed_task_logger


class DistributedTask:
    """
    Represents a distributed task that can be executed by multiple workers but only one worker at a time
    """

    def __init__(
        self,
        name: str,
        task_func: Callable[..., Awaitable[None]],
        redis: 'Redis[Any]',
        task_period: float,  # [sec]
        locker_timeout: float = 5 * 60.,  # [sec]
        logging_period: float = 5 * 60.,  # [sec]
        locker_name: str | None = None,
        logger: logging.Logger = distributed_task_logger,
        **kwargs: Any
    ) -> None:
        """
        Initializes a distributed task.

        :param name: A string representing the name of the task.
        :param task_func: An async function that should be executed by the task.
        :param redis: A Redis client instance.
        :param locker_name: A string representing the name of the lock that will be used to
            synchronize the workers.
        :param task_period: A float representing the time interval between each task execution.
        :param locker_timeout: A float representing the maximum amount of time a worker is allowed to
            hold the lock. If a worker fails to release the lock within the specified timeout,
            the lock will be automatically released by Redis.
        :param logging_period: A float representing the time interval between each log message.
        :param logger: A logger instance used for logging.
        :param kwargs: Any additional arguments that should be passed to the task_func when executed.
        """
        self._func = task_func
        self._redis = redis
        self._kwargs = kwargs

        self._logging_period = logging_period
        self._task_period = task_period
        self._locker_timeout = locker_timeout

        self._name = name
        self._locker_name = locker_name or name

        self._is_stopped = True
        self._asyncio_task: asyncio.Task[None] | None = None
        self._locker: Lock = Lock(
            redis=self._redis,
            name=self._locker_name,
            timeout=self._locker_timeout,
            blocking=True,
        )
        self._last_log: datetime | None = None
        self._logger = logger

    async def start(self) -> None:
        """
        Start the task.
        """
        if not self._is_stopped:
            raise RuntimeError(f"task '{self._name}' is already running.")

        self._asyncio_task = asyncio.create_task(self._run())
        self._is_stopped = False

    async def stop(self, timeout: float = 5.) -> None:
        """
        Stop the task.
        :param timeout:  A float representing the maximum amount of time to wait for the task
            to stop before forcefully cancelling it.
        """
        if self._asyncio_task is None or self._is_stopped:
            return

        self._is_stopped = True
        try:
            await asyncio.wait_for(self._asyncio_task, timeout=timeout)
        except asyncio.TimeoutError:
            self._logger.error(f"task '{self._name}' was interrupted while being processed.")

        try:
            await self._locker.release()
        except LockError:
            pass

        self._asyncio_task = None

    async def _log_with_timeout(self) -> None:
        """
        Logs the status of the task at an interval defined by the `logging_period` parameter.
        If the last time a log was made is greater than the `logging_period`, a new log is created with the name
         of the task and the message that it is running. The current time is recorded as the last log time.

        If this method is called again before `logging_period` has elapsed, nothing happens.
        """
        now = datetime.utcnow()
        if self._last_log is None or now - self._last_log > timedelta(seconds=self._logging_period):
            self._logger.info(f"task '{self._name}' is running.")
            self._last_log = now

    async def _run(self) -> None:
        """
        Runs the distributed task.

        This method is responsible for periodically executing the task
        and extending the Redis lock that keeps the task exclusive to
        a single worker. It also logs the status of the task and its
        duration and handles exceptions that occur during execution.

        If the `stop` method is called, this method will stop the task.
        """
        try:
            while not self._is_stopped:
                try:
                    async with self._locker:
                        await self._log_with_timeout()
                        started_at = datetime.utcnow()
                        await self._func(**self._kwargs)

                        elapsed_time = datetime.utcnow() - started_at
                        remaining_time = self._task_period - elapsed_time.total_seconds()
                        if remaining_time > 0:
                            await asyncio.sleep(remaining_time)

                except asyncio.CancelledError:
                    self._logger.exception(f"task '{self._name}' was interrupted.")
                    raise
                except Exception:
                    self._logger.exception(f"task '{self._name}' failed.")
                    await asyncio.sleep(self._task_period or 0.)

        except asyncio.CancelledError:
            self._logger.info(f"task '{self._name}' cancelled")
            raise
