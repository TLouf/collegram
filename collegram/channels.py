from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from telethon.errors import ChannelPrivateError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import PeerChannel

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel, ChatFull, Message

logger = logging.getLogger(__name__)


async def query_bot(client: TelegramClient, bot, cmd):
    async with client.conversation(bot) as conv:
        await conv.send_message(cmd)
        return await conv.get_response()

def search_from_tgdb(client: TelegramClient, query):
    search_res = client.loop.run_until_complete(query_bot(client, 'tgdb_bot', f"/search {query}"))
    return re.findall(r'@([a-zA-Z0-9_]+)', search_res.message)

def search_from_api(client: TelegramClient, query, limit=100):
    return [c.username for c in client(SearchRequest(q=query, limit=limit)).chats]

def get(client: TelegramClient, channel_id: int | str) -> Channel | None:
    # channel_id can be integer ID or username
    try:
        return client.get_entity(PeerChannel(channel_id))
    except ChannelPrivateError:
        logger.info(f"found private channel {channel_id}")


def get_full(client: TelegramClient, channel_id: int | str) -> ChatFull | None:
    # channel = get(client, channel_id)
    # if channel is not None:
    try:
        return client(GetFullChannelRequest(channel=channel_id))
    except ChannelPrivateError:
        logger.info(f"found private channel {channel_id}")


def from_forwarded(client: TelegramClient, messages: list[Message]) -> set[str]:
    new_channels = set()

    for m in messages:
        if m.fwd_from is not None:
            fwd_from_entity = m.fwd_from.from_id
            if isinstance(fwd_from_entity, PeerChannel):
                channel_obj = get(client, fwd_from_entity.channel_id)
                if channel_obj is not None:
                    new_channels.add(channel_obj.id)

    return new_channels
