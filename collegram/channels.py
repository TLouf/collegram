from __future__ import annotations

import datetime
import json
import logging
import re
import time
import typing

import polars as pl
from telethon.errors import ChannelPrivateError, UsernameInvalidError
from telethon.tl.functions.channels import (
    GetChannelRecommendationsRequest,
    GetFullChannelRequest,
)
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import (
    Channel,
    ChannelFull,
    ChatFull,
    InputPeerChannel,
    InputPeerUser,
    PeerChannel,
)

import collegram.json
import collegram.messages
import collegram.text
import collegram.users
from collegram.utils import LOCAL_FS, PY_PL_DTYPES_MAP

if typing.TYPE_CHECKING:
    from pathlib import Path

    from fsspec import AbstractFileSystem
    from lingua import LanguageDetector
    from telethon import TelegramClient
    from telethon.tl.types import (
        TypeChat,
        TypeInputChannel,
        TypeInputPeer,
    )

logger = logging.getLogger(__name__)


async def query_bot(client: TelegramClient, bot, cmd):
    async with client.conversation(bot, timeout=120) as conv:
        await conv.send_message(cmd)
        return await conv.get_response()


def search_from_tgdb(client: TelegramClient, query):
    while True:
        search_res = client.loop.run_until_complete(
            query_bot(client, "tgdb_bot", f"/search {query}")
        )
        if "result" in search_res.message:
            results = re.findall(r"@([a-zA-Z0-9_]+)", search_res.message)
            break
        elif "exhausted your daily free searches" in search_res.message:
            raise RuntimeError(search_res.message)
        else:
            time.sleep(10)

    id_access_hash_map = {}
    for username in results:
        entity = get_input_peer(client, username)
        if hasattr(entity, 'channel_id'):
            id_access_hash_map[entity.channel_id] = entity.access_hash
    return id_access_hash_map


def search_from_api(client: TelegramClient, query, limit=100):
    return {c.id: c.access_hash for c in client(SearchRequest(q=query, limit=limit)).chats}


@typing.overload
def get_input_peer(
    client: TelegramClient, channel_id: str, access_hash: int | None
) -> TypeInputPeer:
    ...


@typing.overload
def get_input_peer(
    client: TelegramClient, channel_id: int, access_hash: int
) -> InputPeerChannel:
    ...


@typing.overload
def get_input_peer(
    client: TelegramClient, channel_id: int, access_hash: None
) -> PeerChannel:
    ...


def get_input_peer(
    client: TelegramClient, channel_id: str | int, access_hash: int | None = None
):
    if isinstance(channel_id, str) and channel_id.isdigit():
        channel_id = int(channel_id)

    if isinstance(channel_id, str):
        try:
            # Using `get_input_entity` instead of just passing the str through to avoid
            # skipping too many errors by catching on `GetFullChannelRequest`, which
            # doesn't allow to know if no peer was found, or if wrong type of peer was.
            return client.get_input_entity(channel_id)
        except (UsernameInvalidError, ValueError):
            logger.error(f'No peer has "{channel_id}" as username')
    elif access_hash is None:
        return PeerChannel(channel_id)
    else:
        return InputPeerChannel(channel_id, access_hash)


def get(
    client: TelegramClient,
    channel: int | str,
    access_hash: int | None = None,
) -> Channel | None:
    input_chan = get_input_peer(client, channel, access_hash)
    if input_chan:
        try:
            return client.get_entity(input_chan)
        except ChannelPrivateError:
            channel_id = channel.id if isinstance(channel, Channel) else channel
            logger.debug(f"found private channel {channel_id}")
            return


def get_full(
    client: TelegramClient,
    channels_dir: Path,
    anon_func,
    channel: Channel | PeerChannel | None = None,
    channel_id: int | str | None = None,
    access_hash: int | None = None,
    force_query=False,
    fs: AbstractFileSystem = LOCAL_FS,
) -> tuple[ChatFull | None, dict]:
    full_chat = None
    if channel_id is None and channel is None:
        raise ValueError("Either `channel` or `channel_id` must be set.")
    elif channel_id is None:
        channel_id = channel.id if isinstance(channel, Channel) else channel.channel_id

    # Won't find file if `channel_id` is a username, but it's ok for our usage since we
    # always force a query for the channels in the initial seed, which are the only ones
    # we refer to with their usernames at first. Anyway, it just implies a request more.
    anon_id = anon_func(channel_id)
    save_path = str(channels_dir / f"{anon_id}.json")
    full_chat_d = (
        json.loads(fs.open(save_path, "r").read()) if fs.exists(save_path) else {}
    )
    if full_chat_d:
        chat = get_matching_chat_from_full(full_chat_d)
        access_hash = chat["access_hash"]

    if force_query or not full_chat_d:
        input_chan = (
            channel
            if isinstance(channel, (Channel, PeerChannel))
            else get_input_peer(client, channel_id, access_hash)
        )
        str_id_is_user = isinstance(input_chan, InputPeerUser)
        if input_chan and str_id_is_user:
            logger.error(f"Passed identifier {channel_id} refers to a user.")
        elif input_chan:
            try:
                full_chat = client(GetFullChannelRequest(channel=input_chan))
                full_chat_d = {**full_chat_d, **get_full_anon_dict(full_chat, anon_func)}
                anon_id = full_chat_d["full_chat"]["id"]
                p = str(channels_dir / f"{anon_id}.json")
                fs.open(p, "w").write(json.dumps(full_chat_d))
            except ChannelPrivateError:
                logger.debug(f"found private channel {channel_id}")
            except ValueError:
                logger.error("unexpected valuerror")
                breakpoint()
    return full_chat, full_chat_d


def content_count(client: TelegramClient, channel: TypeInputChannel, content_type: str):
    f = collegram.messages.MESSAGE_CONTENT_TYPE_MAP[content_type]
    return collegram.messages.get_channel_messages_count(client, channel, f)


def get_recommended(
    client: TelegramClient, channel: TypeInputChannel
) -> list[TypeChat]:
    return client(GetChannelRecommendationsRequest(channel)).chats


@typing.overload
def get_matching_chat_from_full(full_chat: ChatFull, channel_id: int | None = None) -> Channel:
    ...


@typing.overload
def get_matching_chat_from_full(full_chat: dict, channel_id: int | None = None) -> dict:
    ...


def get_matching_chat_from_full(full_chat: ChatFull | dict, channel_id: int | None = None) -> Channel | dict:
    if isinstance(full_chat, dict):
        get = lambda obj, s: obj.get(s)
    else:
        get = lambda obj, s: getattr(obj, s)

    id_to_match = get(get(full_chat, "full_chat"), "id") if channel_id is None else channel_id
    chat = [
        c
        for c in get(full_chat, "chats")
        if get(c, "id") == id_to_match
    ][0]
    return chat


def recover_fwd_from_msgs(
    messages_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
) -> dict[int, dict]:
    chans_fwd_msg = {}
    messages_path = str(messages_path)
    if fs.isdir(messages_path):
        fpaths_iter = fs.glob(f"{messages_path}/*.jsonl")
    elif fs.exists(messages_path):
        fpaths_iter = [messages_path]
    else:
        fpaths_iter = []

    for p in fpaths_iter:
        for m in collegram.json.yield_message(
            p, fs, collegram.json.FAST_FORWARD_DECODER
        ):
            if m.fwd_from is not None:
                from_chan_id = getattr(m.fwd_from.from_id, "channel_id", None)
                if from_chan_id is not None:
                    chans_fwd_msg[from_chan_id] = {"id": m.id}
                    if m.reply_to is not None:
                        chans_fwd_msg[from_chan_id][
                            "reply_to"
                        ] = m.reply_to.reply_to_msg_id

    return chans_fwd_msg


def fwd_from_msg_ids(
    client: TelegramClient,
    channels_dir: Path,
    chat: TypeInputChannel,
    chans_fwd_msg: dict[int, dict],
    anonymiser,
    parent_priority,
    lang_detector: LanguageDetector,
    lang_priorities: dict,
    private_chans_priority: int,
    fs: AbstractFileSystem = LOCAL_FS,
):
    forwarded_channels = {}
    for chan_id, m_d in chans_fwd_msg.items():
        fwd_full_chan_d = {}
        m = client.get_messages(
            entity=chat, ids=m_d["id"], reply_to=m_d.get("reply_to")
        )
        fwd_from = getattr(m, "fwd_from", None)
        if fwd_from is not None:
            _, fwd_full_chan_d = get_full(
                client,
                channels_dir,
                anonymiser.anonymise,
                channel=m.fwd_from.from_id,
                fs=fs,
            )
        elif m is not None:
            logger.error("message supposed to have been forwarded is not")
            breakpoint()
        else:
            logger.error("forwarded message was deleted")

        prio = get_explo_priority(
            fwd_full_chan_d,
            anonymiser,
            parent_priority,
            lang_detector,
            lang_priorities,
            private_chans_priority,
        )
        forwarded_channels[chan_id] = prio
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
    full_chat.migrated_from_chat_id = anon_func(
        full_chat.migrated_from_chat_id, safe=safe
    )
    return full_chat


def get_full_anon_dict(full_chat: ChatFull, anon_func, safe=True):
    # TODO: anon mentions in about
    channel_save_data = json.loads(full_chat.to_json())
    for c in channel_save_data["chats"]:
        c["photo"] = None
        c["id"] = anon_func(c["id"], safe=safe)
        c["username"] = anon_func(c["username"], safe=safe)
        c["title"] = anon_func(c["title"], safe=safe)
        if c["usernames"] is not None:
            for un in c["usernames"]:
                un["username"] = anon_func(un["username"], safe=safe)
    full_channel = channel_save_data["full_chat"]
    full_channel["chat_photo"] = None
    full_channel["id"] = anon_func(full_channel["id"], safe=safe)
    full_channel["linked_chat_id"] = anon_func(
        full_channel["linked_chat_id"], safe=safe
    )
    full_channel["migrated_from_chat_id"] = anon_func(
        full_channel["migrated_from_chat_id"], safe=safe
    )
    return channel_save_data


def get_explo_priority(
    fwd_full_chan_d: dict,
    anonymiser,
    parent_priority: int,
    lang_detector: LanguageDetector,
    lang_priorities: dict,
    private_chans_priority: int,
):
    if fwd_full_chan_d:
        lang = collegram.text.detect_chan_lang(
            fwd_full_chan_d, anonymiser.inverse_anon_map, lang_detector
        )
        # Some channels may be from a relevant language, but detection was just not
        # conclusive, so default shouldn't be too high.
        lang_prio = lang_priorities.get(lang, 100)
        # lang_prio is both increment and multiplicative factor, thus if some language has
        # prio value N times superior, after exploring N of other language, it'l' be this
        # language's turn.
        prio = lang_prio * parent_priority + lang_prio
    else:
        prio = private_chans_priority
    return prio


def get_extended_save_data(
    client: TelegramClient,
    channel_full: ChatFull,
    channel_saved_data: dict,
    anonymiser,
    channels_dir: Path,
    recommended_chans_prios: dict | None,
    **explo_prio_kwargs,
):
    chat = get_matching_chat_from_full(channel_full)

    users_list = collegram.users.get_channel_users(
        client, chat, anonymiser.anonymise
    ) if channel_full.full_chat.can_view_participants else []

    # Might seem redundant to save before and after getting messages, but this
    # way, if the connection crashes this info will still have been saved.
    channel_save_data = get_full_anon_dict(
        channel_full, anonymiser.anonymise
    )
    channel_save_data['participants'] =  [json.loads(u.to_json()) for u in users_list]

    channel_save_data['recommended_channels'] = []
    for c in get_recommended(client, chat):
        _, full_chat_d = get_full(
            client, channels_dir, anonymiser.anonymise, channel=c,
        )
        if recommended_chans_prios is not None:
            recommended_chans_prios[c.id] = get_explo_priority(
                full_chat_d, anonymiser, **explo_prio_kwargs
            )
        channel_save_data['recommended_channels'].append(anonymiser.anonymise(c.id))

    for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items():
        count = collegram.messages.get_channel_messages_count(client, chat, f)
        channel_save_data[f"{content_type}_count"] = count

    channel_save_data['forwards_from'] = channel_saved_data.get('forwards_from', [])
    anonymiser.save_map()
    channel_save_data['last_queried_at'] = datetime.datetime.now(datetime.UTC).isoformat()
    return channel_save_data


def save(chan_data: dict, channels_dir: Path, fs: AbstractFileSystem = LOCAL_FS):
    anon_id = chan_data['full_chat']['id']
    channel_save_path = channels_dir / f"{anon_id}.json"
    fs.mkdirs(str(channel_save_path.parent), exist_ok=True)
    with fs.open(str(channel_save_path), "w") as f:
        json.dump(chan_data, f)


DISCARDED_CHAN_FULL_FIELDS = (
    "_",
    "notify_settings",
    "call",
    "groupcall_default_join_as",
    "stories",
    "exported_invite",
    "default_send_as",
    "available_reactions",
    "bot_info",
    "stickerset",
    "chat_photo",
    "sticker_set_id",
    "location",
    "recent_requesters",
    "pending_suggestions",
)
DISCARDED_CHAN_FIELDS = (
    "default_banned_rights",
    "banned_rights",
    "admin_rights",
    "color",
    "restriction_reason",
    "photo",
)
CHANGED_CHAN_FIELDS = {
    "id": pl.Utf8,
    "linked_chat_id": pl.Utf8,
    "migrated_from_chat_id": pl.Utf8,
    "forwards_from": pl.List(pl.Utf8),
    "usernames": pl.List(pl.Utf8),
    "migrated_to": pl.Utf8,
}
NEW_CHAN_FIELDS = {
    "bot_ids": pl.List(pl.Int64),
    "linked_chats_ids": pl.List(pl.Utf8),
    "location_point": pl.List(pl.Float64),
    "location_str": pl.Utf8,
    "last_queried_at": pl.Datetime,
    "sticker_set_id": pl.Int64,
    **{
        f"{content_type}_count": pl.Int64
        for content_type in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.keys()
    },
}


def flatten_dict(c: dict) -> tuple[dict, list | None]:
    flat_c = {**get_matching_chat_from_full(c), **c["full_chat"]}
    flat_c["date"] = datetime.datetime.fromisoformat(flat_c["date"])
    last_queried_at = c.get("last_queried_at")
    flat_c["last_queried_at"] = (
        datetime.datetime.fromisoformat(last_queried_at)
        if last_queried_at is not None
        else None
    )
    flat_c["forwards_from"] = c.get("forwards_from")
    flat_c["recommended_channels"] = c.get("recommended_channels")
    for content_type in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.keys():
        count_key = f"{content_type}_count"
        flat_c[count_key] = c.get("count_key")
    flat_c["linked_chats_ids"] = [
        chat["id"] for chat in c["chats"] if chat["id"] != c["full_chat"]["id"]
    ]
    # From chanfull:
    flat_c["bot_ids"] = flat_c.pop("bot_info")
    for i in range(len(flat_c["bot_ids"])):
        flat_c["bot_ids"][i] = flat_c["bot_ids"][i].get("user_id")

    flat_c["sticker_set_id"] = flat_c.pop("stickerset", None)
    if flat_c["sticker_set_id"] is not None:
        flat_c["sticker_set_id"] = flat_c["sticker_set_id"]["id"]

    location = flat_c.pop("location", None)
    flat_c["location_point"] = None
    flat_c["location_str"] = None
    if not (location is None or location["_"] == "ChannelLocationEmpty"):
        point = location["geo_point"]
        if "long" in point and "lat" in point:
            flat_c["location_point"] = [point["long"], point["lat"]]
        flat_c["location_str"] = location["address"]

    flat_c["usernames"] = flat_c.pop("usernames", [])
    for i, uname in enumerate(flat_c["usernames"]):
        flat_c["usernames"][i] = uname["username"]

    migrated_to = flat_c.get("migrated_to")
    if migrated_to is not None:
        flat_c["migrated_to"] = migrated_to["channel_id"]

    for f in DISCARDED_CHAN_FIELDS + DISCARDED_CHAN_FULL_FIELDS:
        flat_c.pop(f, None)
    return flat_c


def get_pl_schema():
    chan_schema = {}
    annots = {
        **Channel.__init__.__annotations__,
        **ChannelFull.__init__.__annotations__,
    }
    discarded_args = DISCARDED_CHAN_FIELDS + DISCARDED_CHAN_FULL_FIELDS
    for arg in set(annots.keys()).difference(discarded_args):
        dtype = annots[arg]
        inner_dtype = typing.get_args(dtype)
        inner_dtype = inner_dtype[0] if len(inner_dtype) > 0 else dtype
        chan_schema[arg] = PY_PL_DTYPES_MAP.get(inner_dtype)
    chan_schema = {**chan_schema, **CHANGED_CHAN_FIELDS, **NEW_CHAN_FIELDS}
    return chan_schema
