from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

from telethon.errors import ChannelPrivateError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import PeerChannel

from collegram.messages import ExtendedMessage

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel, ChannelFull, Message

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


def get_full(client: TelegramClient, channel_id: int | str) -> ChannelFull | None:
    try:
        return client(GetFullChannelRequest(channel=channel_id))
    except ChannelPrivateError:
        logger.info(f"found private channel {channel_id}")


def from_forwarded(messages: list[Message]) -> set[str]:
    new_channels = {
        m.raw_fwd_from_channel_id
        for m in messages
        if isinstance(m, ExtendedMessage) and m.raw_fwd_from_channel_id is not None
    }
    return new_channels


def get_chat_save_dict(chat: Channel, forwarded_channels, anon_func, safe=True) -> dict:
    chat_dict = json.loads(anonymise_chat(chat, anon_func, safe=safe).to_json())
    chat_dict['forwards_from'] = [
        anon_func(c) for c in forwarded_channels
    ]
    return chat_dict

def anonymise_chat(chat: Channel, anon_func, safe=True) -> Channel:
    chat.photo = None
    chat.id = anon_func(chat.id, safe=safe)
    chat.username = anon_func(chat.username, safe=safe)
    chat.title = anon_func(chat.title, safe=safe)
    if chat.usernames is not None:
        chat.usernames = [anon_func(username, safe=safe) for username in chat.usernames]
    return chat

def anonymise_full_chat(full_chat: ChannelFull, anon_func, safe=True) -> ChannelFull:
    full_chat.chat_photo = None
    full_chat.id = anon_func(full_chat.id, safe=safe)
    full_chat.linked_chat_id = anon_func(full_chat.linked_chat_id, safe=safe)
    full_chat.migrated_from_chat_id = anon_func(full_chat.migrated_from_chat_id, safe=safe)
    return full_chat
