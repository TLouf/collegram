from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime

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
