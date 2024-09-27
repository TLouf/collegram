from __future__ import annotations

import datetime
import inspect
import json
import logging
import re
import time
import typing

import polars as pl
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
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
    PeerChannel,
)

import collegram.json
import collegram.messages
import collegram.text
import collegram.users
from collegram.paths import ChannelPaths, ProjectPaths
from collegram.utils import LOCAL_FS, HMAC_anonymiser

if typing.TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from telethon import TelegramClient
    from telethon.tl.types import (
        TypeChat,
        TypeInputChannel,
    )


logger = logging.getLogger(__name__)


async def query_bot(client: TelegramClient, bot, cmd):
    async with client.conversation(bot, timeout=120) as conv:
        await conv.send_message(cmd)
        return await conv.get_response()


def search_from_tgdb(client: TelegramClient, query, raise_on_daily_limit=True):
    while True:
        search_res = client.loop.run_until_complete(
            query_bot(client, "tgdb_bot", f"/search {query}")
        )
        if "result" in search_res.message:
            results = re.findall(r"@([a-zA-Z0-9_]+)", search_res.message)
            break
        elif "exhausted your daily free searches" in search_res.message:
            if raise_on_daily_limit:
                raise RuntimeError(search_res.message)
            else:
                time.sleep(24 * 3600)
        else:
            time.sleep(10)

    id_access_hash_map = {}
    for username in results:
        try:
            entity = get_input_peer(client, username)
        except (ValueError, UsernameInvalidError, ChannelPrivateError):
            continue
        if hasattr(entity, "channel_id"):
            id_access_hash_map[entity.channel_id] = entity.access_hash
    return id_access_hash_map


def search_from_api(client: TelegramClient, query, limit=100):
    return {
        c.id: c.access_hash
        for c in client.loop.run_until_complete(
            client(SearchRequest(q=query, limit=limit))
        ).chats
    }


def get(
    client: TelegramClient,
    input_chan: PeerChannel | InputPeerChannel | str,
) -> Channel | None:
    return client.loop.run_until_complete(client.get_entity(input_chan))


def _get_input_peer(
    client: TelegramClient,
    channel_username: str | None = None,
    channel_id: int | None = None,
    access_hash: int | None = None,
    check: bool = True,
) -> InputPeerChannel:
    """
    Raises:
      - UsernameInvalidError or ValueError when username is wrong
      - ChannelInvalidError if wrong int ID / access_hash pair is passed and check is True
      - ChannelPrivateError if channel is private and check is True
    """
    if channel_id is not None and access_hash is not None:
        peer = InputPeerChannel(channel_id, access_hash)
    elif channel_username is not None:
        peer = channel_username
    elif channel_id is not None:
        peer = PeerChannel(channel_id)
    else:
        raise ValueError("One of channel_username or channel_id must be passed")

    # If we pass a username, `get_input_entity` will check if it exists, however it
    # won't check anything if we pass it a peer. Thus why in that case we need to
    # manually check for existence with a `get_entity`.
    input_entity = client.loop.run_until_complete(client.get_input_entity(peer))
    if not isinstance(peer, str) and check:
        get(client, input_entity)
    return input_entity


def get_input_peer(
    client: TelegramClient,
    channel_username: str | None = None,
    channel_id: int | None = None,
    access_hash: int | None = None,
    check: bool = True,
) -> InputPeerChannel:
    """
    - if ChannelPrivateError, logic outside to handle (can happen!)
    - UsernameInvalidError, ValueError if (ID, access_hash) pair is invalid for that API
      key, and username has changed -> try other access_hash
    - ChannelInvalidError if (ID, access_hash) pair is invalid and `inverse_anon_map`
      was not provided, or no username (case for discussion groups), or chan ID saved in
      recommended or forwarded, but full_chat_d was not
    - IndexError if ID is missing in inverse_anon_map
    """
    try:
        return _get_input_peer(client, channel_username, channel_id, access_hash, check)
    except ChannelInvalidError as e:
        # Try with username if possible
        if channel_username is not None:
            return _get_input_peer(client, channel_username, check=check)
        raise e


def get_full(
    client: TelegramClient,
    channel: Channel | None = None,
    channel_username: str | None = None,
    channel_id: int | None = None,
    access_hash: int | None = None,
) -> ChatFull | None:
    if channel_id is None and channel is None and channel_username is None:
        raise ValueError(
            "Either `channel` or `channel_id` or `channel_username` must be set."
        )

    if channel is not None:
        input_chan = channel
    else:
        input_chan = get_input_peer(
            client,
            channel_username,
            channel_id,
            access_hash,
        )

    full_chat = client.loop.run_until_complete(
        client(GetFullChannelRequest(channel=input_chan))
    )
    return full_chat


def get_usernames_from_chat_d(chat_d: dict) -> list[str]:
    unames = [chat_d["username"]] if chat_d["username"] is not None else []
    if chat_d.get("usernames"):
        # Happens for channels with multiple usernames (see "@deepfaker")
        unames = unames + [u["username"] for u in chat_d["usernames"] if u["active"]]
    return unames


def content_count(client: TelegramClient, channel: TypeInputChannel, content_type: str):
    f = collegram.messages.MESSAGE_CONTENT_TYPE_MAP[content_type]
    return collegram.messages.get_channel_messages_count(client, channel, f)


def get_recommended(
    client: TelegramClient, channel: TypeInputChannel
) -> list[TypeChat]:
    return client.loop.run_until_complete(
        client(GetChannelRecommendationsRequest(channel))
    ).chats


@typing.overload
def get_matching_chat_from_full(
    full_chat: ChatFull, channel_id: int | None = None
) -> Channel:
    ...


@typing.overload
def get_matching_chat_from_full(full_chat: dict, channel_id: int | None = None) -> dict:
    ...


def get_matching_chat_from_full(
    full_chat: ChatFull | dict, channel_id: int | None = None
) -> Channel | dict:
    if isinstance(full_chat, dict):

        def get(obj, s):
            return obj.get(s)
    else:

        def get(obj, s):
            return getattr(obj, s)

    id_to_match = (
        get(get(full_chat, "full_chat"), "id") if channel_id is None else channel_id
    )
    chat = [c for c in get(full_chat, "chats") if get(c, "id") == id_to_match][0]
    return chat


def get_anoned_full_dict(full_chat: ChatFull, anonymiser: HMAC_anonymiser, safe=True):
    channel_save_data = json.loads(full_chat.to_json())
    return anon_full_dict(channel_save_data, anonymiser, safe=safe)


def anon_full_dict(full_dict: dict, anonymiser: HMAC_anonymiser, safe=True):
    anon_func = anonymiser.anonymise
    for c in full_dict["chats"]:
        c["photo"] = None
        c["username"] = anon_func(c["username"], safe=safe)
        c["title"] = anon_func(c["title"], safe=safe)
        if c["usernames"] is not None:
            for un in c["usernames"]:
                un["username"] = anon_func(un["username"], safe=safe)
    full_channel = full_dict["full_chat"]
    full_channel["chat_photo"] = None

    def user_anon_func(d):
        return collegram.users.anon_user_d(d, anon_func)

    full_dict["users"] = list(map(user_anon_func, full_dict.get("users", [])))
    if "participants" in full_dict:
        full_dict["participants"] = list(map(user_anon_func, full_dict["participants"]))

    return full_dict


def get_explo_priority(
    lang_code: str,
    messages_count: int,
    participants_count: int | None,
    lifespan_seconds: int,
    distance_from_core: int,
    nr_forwarding_channels: int,
    nr_recommending_channels: int,
    nr_linking_channels: int,
    lang_priorities: dict,
    acty_slope: float = 1,
    acty_inflexion: int = 100,  # 100 messages per day
    acty_user_inflexion: float = 0.1,  # 10 messages per day for channel of 100 users
):
    # Returns a score between 0 and 1. Here the lowest the returned score, the more
    # priority the channel is given for message collection.
    lang_score = lang_priorities.get(lang_code, 1)
    if participants_count is None:
        acty_per_day = messages_count / (lifespan_seconds / 3600 / 24)
        acty_score = 1 / (1 + (acty_inflexion / acty_per_day) ** acty_slope)
    else:
        acty_per_user_day = (
            messages_count / participants_count / (lifespan_seconds / 3600 / 24)
        )
        acty_score = 1 / (1 + (acty_user_inflexion / acty_per_user_day) ** acty_slope)
    # With the following, for instance if channel is forwarded from by two core channels
    # (dist = 0), it will be prioritised over other core channels. And if channel is
    # recommended in / fowarded from n times more than another at the same distance from
    # the core, it will have a priority ~n times smaller.
    central_score = (distance_from_core + 1) / (
        1 + nr_recommending_channels + nr_forwarding_channels + nr_linking_channels
    )
    # We threshold acty_score to 1e-3 so that it doesn't dominate all others.
    return lang_score * max(acty_score, 1e-3) * min(central_score, 1)


def save(
    chan_data: dict,
    project_paths: ProjectPaths,
    key_name: str | None,
    fs: AbstractFileSystem = LOCAL_FS,
):
    anon_id = chan_data["full_chat"]["id"]
    chan_paths = ChannelPaths(anon_id, project_paths)
    channel_save_path = chan_paths.channel
    # Since `access_hash` is API-key-dependent, always add a key_name: access_hash
    # mapping in `access_hashes`.
    if key_name is not None:
        for chat_d in chan_data["chats"]:
            access_hashes = chat_d.get("access_hashes", {})
            if (
                key_name not in access_hashes
                and chat_d["access_hash"] not in access_hashes.values()
            ):
                access_hashes[key_name] = chat_d["access_hash"]
                chat_d["access_hashes"] = access_hashes
    fs.mkdirs(str(channel_save_path.parent), exist_ok=True)
    with fs.open(str(channel_save_path), "w") as f:
        json.dump(chan_data, f)


def load(
    anon_id: str, project_paths: ProjectPaths, fs: AbstractFileSystem = LOCAL_FS
) -> dict:
    chan_paths = ChannelPaths(anon_id, project_paths)
    save_path = str(chan_paths.channel)
    full_chat_d = (
        json.loads(fs.open(save_path, "r").read()) if fs.exists(save_path) else {}
    )
    return full_chat_d


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
    "wallpaper",
)
DISCARDED_CHAN_FIELDS = (
    "default_banned_rights",
    "banned_rights",
    "admin_rights",
    "color",
    "restriction_reason",
    "photo",
    "emoji_status",
    "profile_color",
    "access_hashes",
)
CHANGED_CHAN_FIELDS = {
    "usernames": pl.List(pl.Utf8),
}
NEW_CHAN_FIELDS = {
    "bot_ids": pl.List(pl.Int64),
    "linked_chats_ids": pl.List(pl.Int64),
    "recommended_channels": pl.List(pl.Int64),
    "location_point": pl.List(pl.Float64),
    "location_str": pl.Utf8,
    "last_queried_at": pl.Datetime,
    "sticker_set_id": pl.Int64,
    **{
        f"{content_type}_count": pl.Int64
        for content_type in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.keys()
    },
}


def flatten_dict(c: dict) -> dict:
    flat_c = {**get_matching_chat_from_full(c), **c["full_chat"]}
    flat_c["date"] = datetime.datetime.fromisoformat(flat_c["date"])
    last_queried_at = c.get("last_queried_at")
    flat_c["last_queried_at"] = (
        datetime.datetime.fromisoformat(last_queried_at)
        if isinstance(last_queried_at, str)
        else last_queried_at
    )
    flat_c["recommended_channels"] = c.get("recommended_channels")
    for content_type in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.keys():
        count_key = f"{content_type}_count"
        flat_c[count_key] = c.get(count_key)
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
    annots = {
        **inspect.getfullargspec(Channel).annotations,
        **inspect.getfullargspec(ChannelFull).annotations,
    }
    discarded_args = DISCARDED_CHAN_FIELDS + DISCARDED_CHAN_FULL_FIELDS
    chan_schema = {
        arg: collegram.utils.py_to_pl_types(annots[arg])
        for arg in set(annots.keys()).difference(discarded_args)
    }
    chan_schema = {**chan_schema, **CHANGED_CHAN_FIELDS, **NEW_CHAN_FIELDS}
    return chan_schema


def erase(
    channel_paths: ChannelPaths,
    fs: AbstractFileSystem = LOCAL_FS,
):
    """
    Remove all data about channel from disk (for deleted channels or those who became
    private).
    """
    if fs.exists(channel_paths.anon_map):
        fs.rm(channel_paths.anon_map)
    if fs.exists(channel_paths.channel):
        fs.rm(channel_paths.channel)
    if fs.exists(channel_paths.messages):
        for p in fs.ls(channel_paths.messages):
            fs.rm(p)
        fs.rmdir(channel_paths.messages)
