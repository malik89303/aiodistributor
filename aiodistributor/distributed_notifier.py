from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterable

from redis.asyncio import Redis
from redis.exceptions import ResponseError


class DistributedNotifier:
    def __init__(
        self,
        redis: 'Redis[Any]',
    ) -> None:
        self._redis: Redis[Any] = redis
        self._last_message_id: str | None = None

    async def get_last_stream_message(
        self,
        stream_id: str,
        offset: int = 1,
    ) -> tuple[str | None, str | None]:
        """
        :param stream_id: id of stream
        :param offset: offset relative to the end (posititve)
        :return: tuple of last message id and message, using offset, or tuple of 2 none in case of block timeout
        exceeded
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
        last_message_id: str | None,
        block_timeout: int | None = 60,  # [sec]
    ) -> tuple[str | None, str | None]:
        """
        :param stream_id: id of stream
        :param last_message_id: id of last message, if not specified last in stream will be returned
        :param block_timeout: timeout in seconds to read response from redis
        :return: tuple of message id and message, or tuple of 2 none in case of block timeout exceeded
        """
        result = await self._redis.xread(
            streams={stream_id: last_message_id or '$'},
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
        :param stream_id: id of stream
        :param message: str message to insert
        :param timestamp: timestamp of message (if newer exists in stream, message won't be inserted)
        :param max_len: max number of messages to store in stream, if None - messages won't be deleted
        :return: returns id of inserted message, or None in case of outdated message
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

    async def get_message(self, stream_id: str, block_timeout: int | None = 60) -> AsyncIterable[str]:
        """
        asynchronous generator designed to iterate over it to receive new messages from the stream
        :param stream_id: id of stream
        :param block_timeout: timeout in seconds to read response from redis
        :return: string of message
        """
        message_id, message = await self.get_last_stream_message(stream_id=stream_id)
        while True:
            if message is not None and message_id is not None:
                yield message

            message_id, message = await self.wait_message_from_stream(
                stream_id=stream_id,
                last_message_id=message_id,
                block_timeout=block_timeout,
            )
