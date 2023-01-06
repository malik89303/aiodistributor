from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from aiodistributor.common.loggers import distributed_waiter_logger


class DistributedNotifier:
    def __init__(
        self,
        redis: 'Redis[Any]',
        logger: logging.Logger = distributed_waiter_logger,
    ) -> None:
        self._redis: Redis[Any] = redis

    async def get_last_stream_message(
        self,
        stream_id: str,
        offset: int = 1,
    ) -> tuple[str | None, str | None]:
        """
        return: tuple of last message id and message, using offset, or tuple of 2 none
         in case of block timeout exceeded
        """

        result = await self._redis.xrevrange(
            name=stream_id,
            max='+',
            min='-',
            count=offset,
        )

        try:
            message_id, message = result[-1]
        except (TypeError, IndexError):
            return None, None

        return message_id, message['data']

    async def wait_message_from_stream(
        self,
        stream_id: str,
        after_message_id: str | None,
        block_timeout: int | None = 60,  # [sec]
    ) -> tuple[str | None, str | None]:
        """
        return: tuple of message id and message, or tuple of 2 none in case of block timeout exceeded
        """
        result = await self._redis.xread(
            streams={stream_id: after_message_id or '$'},
            block=(block_timeout * 1000) if block_timeout else None,
            count=1,
        )
        if not result:
            return None, None
        message_id, message = result[0][1][0]
        return message_id, message['data']

    async def add_message_to_stream_if_newer(
        self,
        stream_id: str,
        message: str,
        timestamp: datetime | None = None,
        max_len: int | None = 5,
    ) -> str | None:
        """
        return: returns id of inserted message, or None in case of outdated message
        """
        try:
            return await self._redis.xadd(
                name=stream_id,
                fields={'data': message},
                approximate=True,
                maxlen=max_len,
                id=f'{int(timestamp.timestamp() * 1000)}-*' if timestamp else '*',
            )
        except ResponseError as e:
            if e.args == ('The ID specified in XADD is equal or smaller than the target stream top item',):
                return None
            raise
