from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, overload

from telethon.errors import ChannelPrivateError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel, InputPeerChannel, PeerChannel

import collegram.json
from collegram.messages import ExtendedMessage

if TYPE_CHECKING:
    from pathlib import Path

    from telethon import TelegramClient
    from telethon.tl.types import ChannelFull, ChatFull, Message

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

@overload
def get_input_peer(channel_id: str, access_hash: int | None) -> str:
    ...
@overload
def get_input_peer(channel_id: int, access_hash: int) -> InputPeerChannel:
    ...
@overload
def get_input_peer(channel_id: int, access_hash: None) -> PeerChannel:
    ...
def get_input_peer(channel_id: str | int, access_hash: int | None = None):
    if isinstance(channel_id, str):
        return channel_id
    elif access_hash is None:
        return PeerChannel(channel_id)
    else:
        return InputPeerChannel(channel_id, access_hash)

def get(
    client: TelegramClient, channel: int | str, access_hash: int | None = None,
) -> Channel | None:
    input_chan = get_input_peer(channel, access_hash)
    try:
        return client.get_entity(input_chan)
    except ChannelPrivateError:
        channel_id = channel.id if isinstance(channel, Channel) else channel
        logger.info(f"found private channel {channel_id}")
        return


def get_full(
    client: TelegramClient, channel: Channel | int | str, access_hash: int | None = None,
    channels_dir: Path | None = None, anon_func_to_save=None
) -> ChannelFull | None:
    input_chan = channel if isinstance(channel, (Channel, PeerChannel)) else get_input_peer(channel, access_hash)
    try:
        full_chat = client(GetFullChannelRequest(channel=input_chan))
        if anon_func_to_save is not None and channels_dir is not None:
            full_chat_d = get_full_anon_dict(full_chat, anon_func_to_save)
            p = channels_dir / f"{full_chat.full_chat.id}.json"
            p.write_text(json.dumps(full_chat_d))
        return full_chat
    except (ChannelPrivateError, ValueError):
        channel_id = channel.id if isinstance(channel, Channel) else channel
        logger.info(f"found private channel {channel_id}")
        return

def get_or_load_full(client: TelegramClient, channel_id: int | str, channels_dir: Path, anon_func_to_save=None, access_hash: int | None = None) -> dict | ChannelFull | None:
    p = channels_dir / f"{channel_id}.json"
    if p.exists():
        # TODO: implement object in json module to have instance of custom class returned here
        return json.loads(p.read_text())
    else:
        return get_full(client, channel_id, access_hash, channels_dir, anon_func_to_save)


def from_forwarded(messages: list[Message]) -> set[int]:
    new_channels = {
        m.raw_fwd_from_channel_id
        for m in messages
        if isinstance(m, ExtendedMessage) and m.raw_fwd_from_channel_id is not None
    }
    return new_channels


def get_chat_save_dict(chat: Channel, anon_func, safe=True) -> dict:
    chat_dict = json.loads(anonymise_chat(chat, anon_func, safe=safe).to_json())
    return chat_dict

def anonymise_chat(chat: Channel, anon_func, safe=True) -> Channel:
    chat.photo = None
    chat.id = anon_func(chat.id, safe=safe)
    chat.username = anon_func(chat.username, safe=safe)
    chat.title = anon_func(chat.title, safe=safe)
    if chat.usernames is not None:
        for un in chat.usernames:
            un.username = anon_func(un.username, safe=safe)
    return chat

def anonymise_full_chat(full_chat: ChannelFull, anon_func, safe=True) -> ChannelFull:
    full_chat.chat_photo = None
    full_chat.id = anon_func(full_chat.id, safe=safe)
    full_chat.linked_chat_id = anon_func(full_chat.linked_chat_id, safe=safe)
    full_chat.migrated_from_chat_id = anon_func(full_chat.migrated_from_chat_id, safe=safe)
    return full_chat

def get_full_anon_dict(full_chat: ChatFull, anon_func, safe=True):
    channel_save_data = json.loads(full_chat.to_json())
    for c in channel_save_data['chats']:
        c['photo'] = None
        c['id'] = anon_func(c['id'], safe=safe)
        c['username'] = anon_func(c['username'], safe=safe)
        c['title'] = anon_func(c['title'], safe=safe)
        if c['usernames'] is not None:
            for un in c['usernames']:
                un['username'] = anon_func(un['username'], safe=safe)
    full_channel = channel_save_data['full_chat']
    full_channel['chat_photo'] = None
    full_channel['id'] = anon_func(full_channel['id'], safe=safe)
    full_channel['linked_chat_id'] = anon_func(full_channel['linked_chat_id'], safe=safe)
    full_channel['migrated_from_chat_id'] = anon_func(full_channel['migrated_from_chat_id'], safe=safe)
    return channel_save_data
