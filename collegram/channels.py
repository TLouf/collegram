from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import PeerChannel

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Message

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

def get(client: TelegramClient, channel_id):
    entity = PeerChannel(channel_id) if channel_id.isdigit() else channel_id
    try:
        channel = client.get_entity(entity)
    except ValueError:
        channel = None
    return channel


def from_forwarded(client: TelegramClient, messages: list[Message]) -> set[str]:
    new_channels = set()

    for m in messages:
        if m.fwd_from is not None:
            fwd_from_entity = m.fwd_from.from_id
            if isinstance(fwd_from_entity, PeerChannel):
                channel_obj = client.get_entity(PeerChannel(fwd_from_entity.channel_id))
                new_channels.add(channel_obj.username)

    return new_channels
