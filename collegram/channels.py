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

if TYPE_CHECKING:
    from pathlib import Path

    from lingua import LanguageDetector
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
    client: TelegramClient, channels_dir: Path,
    channel: Channel | PeerChannel | None = None, channel_id: int | str | None = None,
    access_hash: int | None = None, anon_func_to_save=None, force_query=False
) -> tuple[ChannelFull | None, dict]:
    full_chat = None
    if channel_id is None and channel is None:
        raise ValueError("Either `channel` or `channel_id` must be set.")
    elif channel_id is None:
        channel_id = channel.id if isinstance(channel, Channel) else channel.channel_id

    save_path = channels_dir / f"{channel_id}.json"
    full_chat_d = json.loads(save_path.read_text()) if save_path.exists() else {}
    if full_chat_d:
        chat = [
            c for c in full_chat_d['chats']
            if c['id'] == full_chat_d['full_chat']['id']
        ][0]
        access_hash = chat['access_hash']
    if force_query or not full_chat_d:
    # if force_query and (access_hash is not None or channel is not None):
        input_chan = (
            channel if isinstance(channel, (Channel, PeerChannel))
            else get_input_peer(channel_id, access_hash)
        )
        try:
            full_chat = client(GetFullChannelRequest(channel=input_chan))
            if anon_func_to_save is not None and channels_dir is not None:
                full_chat_d = get_full_anon_dict(full_chat, anon_func_to_save)
                p = channels_dir / f"{full_chat.full_chat.id}.json"
                p.write_text(json.dumps(full_chat_d))
        except (ChannelPrivateError, ValueError):
            logger.info(f"found private channel {channel_id}")
    return full_chat, full_chat_d


def recover_fwd_from_msgs(messages_path: Path) -> dict[int, int]:
    chans_fwd_msg = {}
    if messages_path.is_dir():
        fpaths_iter = messages_path.glob('*.jsonl')
    elif messages_path.exists():
        fpaths_iter = [messages_path]
    else:
        fpaths_iter = []

    for p in fpaths_iter:
        for m in collegram.json.yield_message(p, collegram.json.FAST_FORWARD_DECODER):
            if m.fwd_from is not None:
                from_chan_id = getattr(m.fwd_from.from_id, 'channel_id', None)
                if from_chan_id is not None:
                    chans_fwd_msg[from_chan_id] = m.id

    return chans_fwd_msg

def fwd_from_msg_ids(
    client: TelegramClient, channels_dir: Path, chat: Channel,
    chans_fwd_msg: dict[int, int], anonymiser,
    **priority_kwargs
):
    forwarded_channels = {}
    for chan_id, m_id in chans_fwd_msg.items():
        m = client.get_messages(entity=chat, ids=m_id)
        fwd_from = getattr(m, "fwd_from", None)
        if fwd_from is not None:
            _, fwd_full_chan_d = get_full(
                client, channels_dir, channel=m.fwd_from.from_id,
                anon_func_to_save=anonymiser.anonymise,
            )
        elif m is not None:
            logger.error("message supposed to have been forwarded is not")
            breakpoint()
        else:
            logger.error("forwarded message was deleted")

        if fwd_full_chan_d:
            forwarded_channels[chan_id] = get_explo_priority(
                fwd_full_chan_d, inverse_anon_map=anonymiser.inverse_anon_map, **priority_kwargs
            )
    return forwarded_channels


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
    # TODO: anon mentions in about
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


def get_explo_priority(full_channel_d: dict, lang_detector: LanguageDetector, lang_priorities: dict, inverse_anon_map: dict):
    channel_d = [
        c for c in full_channel_d['chats']
        if c['id'] == full_channel_d['full_chat']['id']
    ][0]
    title = channel_d.get('title', '')
    title = inverse_anon_map.get(title, title)
    clean_text = f"{title}. {full_channel_d['full_chat'].get('about', '')}"
    hash_at_pattern = r'(?:^|\B)((@|#)\w+)(?:$|\b)'
    url_pattern = r'(?:^|\s)(\S+\/t.co\/\S+)(?:$|\b)'
    regex_filter = re.compile('({})|({})'.format(hash_at_pattern, url_pattern))
    clean_text = regex_filter.sub('', clean_text)
    lang = lang_detector.detect_language_of(clean_text)
    if lang is None:
        return 100
    else:
        return lang_priorities.get(lang.iso_code_639_1.name, 100)


