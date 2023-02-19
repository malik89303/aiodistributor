from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterable

from redis.asyncio import Redis
from redis.exceptions import ResponseError


class DistributedNotifier:
    """
    DistributedNotifier class is an asyncio-based implementation of a Redis stream notifier.

    The class provides methods for adding new messages to a stream and receiving messages from a stream.

    The class can also retrieve the last message from a stream and wait for a new message to arrive.
    """

    def __init__(
        self,
        redis: 'Redis[Any]',
    ) -> None:
        """
        :param redis:The Redis client object used to connect to the Redis server.
        """
        self._redis: Redis[Any] = redis
        self._last_message_id: str | None = None

    async def get_last_stream_message(
        self,
        stream_id: str,
        offset: int = 1,
    ) -> tuple[str | None, str | None]:
        """
        Retrieves the last message from a stream.
        :param stream_id: ID of the stream.
        :param offset: The offset relative to the end (positive). Defaults to 1.
        :return:A tuple of the last message ID and message using the offset,
            or a tuple of two Nones in case of block timeout exceeded.
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
         Waits for a new message to arrive in the stream.
        :param stream_id: ID of the stream.
        :param last_message_id: ID of the last message., if not specified last in stream will be returned
        :param block_timeout: timeout in seconds to read the response from Redis. Defaults to 60.
        :return: A tuple of the message ID and message, or a tuple of two Nones in case of block timeout exceeded.
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
        Adds a new message to the stream if it is newer than the existing messages.

        :param stream_id: ID of the stream.
        :param message: message to insert.
        :param timestamp: timestamp of the message. If a newer message exists in the
        :param max_len: maximum number of messages to store in the stream. If None, messages
            won't be deleted. Defaults to 5.
        :return: The ID of the inserted message, or None in case of an outdated message.
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
         An asynchronous generator designed to iterate over it to receive new messages from the stream.

        :param stream_id: ID of the stream.
        :param block_timeout: The timeout in seconds to read the response from Redis. Defaults to 60.

        :return: An asynchronous generator that yields message strings.
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
