from __future__ import annotations

import datetime
import inspect
import logging
from typing import TYPE_CHECKING
from telethon.tl.functions.messages import GetRepliesRequest

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel

logger = logging.getLogger(__name__)


def get_channel_messages(client: TelegramClient, channel: str | Channel, dt_from: datetime.datetime, dt_to: datetime.datetime):
    '''
    date_to exclusive
    '''
    offset_id = 0
    limit = 10000
    all_messages = []
    total_messages = 0
    keep_going = True

    while keep_going:
        chunk_messages = []
        logger.info(f"Current Offset ID is: {offset_id}; Total Messages: {total_messages}")
        # Splitting by 10k chunks in case of disconnection, to at least save something (TODO: save)
        # Telethon docs are misleading, `offset_date` is in fact a datetime.
        messages = client.iter_messages(
            entity=channel,
            offset_date=dt_to,
            offset_id=offset_id,
            limit=limit,
        )

        for message in messages:
            # Take messages in until we've gone further than `date_until` in the past
            # (works because HistoryRequest gets messages in reverse chronological order
            # by default)
            if message.date >= dt_from:
                chunk_messages.append(message)
                if getattr(message, "replies", None) is not None and message.replies.comments:
                    chunk_messages.extend(get_comments(client, channel, message.id))
            else:
                keep_going = False
                break

        if len(chunk_messages) < limit:
            # Needed when reaching first message ever posted
            keep_going = False

        offset_id = message.id
        all_messages.extend(chunk_messages)
        total_messages = len(all_messages)

    return all_messages


def get_comments(client: TelegramClient, channel: str | Channel, message_id):
    result = client(GetRepliesRequest(
        peer=channel,
        msg_id=message_id,
        offset_id=0,
        offset_date=datetime.datetime.now(),
        add_offset=0,
        limit=-1,
        max_id=0,
        min_id=0,
        hash=0
    ))

    comments = [Comment.from_message(m, message_id) for m in result.messages]
    return comments


# First is self, so take from index 1 on.
MESSAGE_INIT_ARGS = inspect.getfullargspec(Message).args[1:]

class Comment(Message):
    # Created this class because m.reply_msg_id did not match the commented-on message's
    # id, so need to save the info somehow.

    @classmethod
    def from_message(cls, message: Message, comments_msg_id: int):
        instance = cls(*[getattr(message, a) for a in MESSAGE_INIT_ARGS])
        instance.comments_msg_id = comments_msg_id
        return instance

    def to_dict(self):
        d = super().to_dict()
        d['comments_msg_id'] = self.comments_msg_id
        return d

