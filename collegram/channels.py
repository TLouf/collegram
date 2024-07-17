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
    InputPeerUser,
    PeerChannel,
)

import collegram.json
import collegram.messages
import collegram.text
import collegram.users
from collegram.paths import ChannelPaths, ProjectPaths
from collegram.utils import LOCAL_FS, HMAC_anonymiser

if typing.TYPE_CHECKING:
    from pathlib import Path

    from bidict import bidict
    from fsspec import AbstractFileSystem
    from lingua import LanguageDetector
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


def get_input_peer(
    client: TelegramClient,
    channel_id: str | int,
    access_hash: int | None = None,
    check: bool = True,
) -> InputPeerChannel:
    """
    Raises:
      - UsernameInvalidError or ValueError when username is wrong
      - ChannelInvalidError if wrong int ID / access_hash pair is passed and check is True
      - ChannelPrivateError if channel is private and check is True
    """
    if isinstance(channel_id, str) and channel_id.isdigit():
        channel_id = int(channel_id)

    if isinstance(channel_id, str):
        peer = channel_id
    elif access_hash is None:
        peer = PeerChannel(channel_id)
    else:
        peer = InputPeerChannel(channel_id, access_hash)

    # If we pass a username, `get_input_entity` will check if it exists, however it
    # won't check anything if we pass it a peer. Thus why in that case we need to
    # manually check for existence with a `get_entity`.
    input_entity = client.loop.run_until_complete(client.get_input_entity(peer))
    if isinstance(channel_id, int) and check:
        client.loop.run_until_complete(client.get_entity(input_entity))
    return input_entity


def get(
    client: TelegramClient,
    channel: int | str,
    access_hash: int | None = None,
) -> Channel | None:
    # TODO: fix
    input_chan = get_input_peer(client, channel, access_hash)
    if input_chan:
        try:
            return client.loop.run_until_complete(client.get_entity(input_chan))
        except ChannelPrivateError:
            channel_id = channel.id if isinstance(channel, Channel) else channel
            logger.debug(f"found private channel {channel_id}")
            return


def get_full(
    client: TelegramClient,
    project_paths: ProjectPaths,
    anonymiser: HMAC_anonymiser,
    key_name: str = "",
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

    anon_id = anonymiser.anonymise(channel_id)
    full_chat_d = load(anon_id, project_paths, fs)

    if force_query or not full_chat_d:
        # TODO: if key_name in access_hashes, use that, otherwise use username. if that
        # doesn't succeed, throw custom error to be caught in caller to redirect this
        # channel to other key
        if isinstance(channel, (Channel, PeerChannel, InputPeerChannel)):
            # It is assumed here that if the caller passes a .*Peer.*, it knows what
            # it's doing and has checked its validity.
            input_chan = channel
        else:
            input_chan = get_input_chan(
                client,
                full_chat_d,
                key_name,
                channel_id,
                access_hash,
                anonymiser.inverse_anon_map,
            )
        str_id_is_user = isinstance(input_chan, InputPeerUser)
        if input_chan and str_id_is_user:
            # This case only happens for firt seed, so we always pass on these.
            logger.error(f"Passed identifier {channel_id} refers to a user.")
        elif input_chan:
            full_chat = client.loop.run_until_complete(
                client(GetFullChannelRequest(channel=input_chan))
            )
            new_full_d = get_anoned_full_dict(full_chat, anonymiser)
            # To avoid overwriting data in channels for which we passed a username, try
            # to load once more here:
            if isinstance(channel_id, str) and not channel_id.isdigit():
                anon_id = anonymiser.anonymise(full_chat.full_chat.id)
                full_chat_d = load(anon_id, project_paths, fs)

            paths = [
                f"chats.id:{chat['id']}.access_hashes" for chat in new_full_d["chats"]
            ]
            full_chat_d = collegram.utils.safe_dict_update(
                full_chat_d, new_full_d, paths
            )
            save(full_chat_d, project_paths, key_name, fs=fs)
    return full_chat, full_chat_d


def get_input_chan(
    client: TelegramClient,
    full_chat_d: dict | None = None,
    key_name: str = "",
    channel_id: str | int | None = None,
    access_hash: int | None = None,
    inverse_anon_map: bidict | None = None,
    username: str | None = None,
):
    """
    - if ChannelPrivateError, logic outside to handle (can happen!)
    - UsernameInvalidError, ValueError if (ID, access_hash) pair is invalid for that API
      key, and username has changed -> try other access_hash
    - ChannelInvalidError if (ID, access_hash) pair is invalid and `inverse_anon_map`
      was not provided, or no username (case for discussion groups), or chan ID saved in
      recommended or forwarded, but full_chat_d was not
    - IndexError if ID is missing in inverse_anon_map
    """
    if channel_id is None and inverse_anon_map is None:
        raise ValueError("must pass either original channel_id or an inverse_anon_map")

    if full_chat_d:
        if channel_id is None and inverse_anon_map is not None:
            anon_id = full_chat_d["full_chat"]["id"]
            channel_id = inverse_anon_map[anon_id]
        if access_hash is None:
            chat = get_matching_chat_from_full(full_chat_d)
            chashes = chat.get("access_hashes", {})
            chash = chat["access_hash"]
            access_hash = chashes.get(key_name) or (
                chash if chash not in chashes.values() else None
            )

    try:
        input_peer = get_input_peer(client, channel_id, access_hash)
    except ChannelInvalidError as e:
        if username is None and full_chat_d and inverse_anon_map is not None:
            unames = get_usernames_from_chat_d(chat)
            uname = None if len(unames) == 0 else unames[0]
            username = inverse_anon_map.get(uname)
        if username is None:
            # Discussion group attached to broadcast channel, can only get with ID
            raise e
        else:
            input_peer = get_input_peer(client, username)
    return input_peer


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


def recover_fwd_from_msgs(
    messages_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
) -> dict[int, dict]:
    chans_fwd_msg = {}
    messages_str_path = str(messages_path)
    if fs.isdir(messages_str_path):
        fpaths_iter = fs.glob(f"{messages_str_path}/*.jsonl")
    elif fs.exists(messages_str_path):
        fpaths_iter = [messages_str_path]
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
    project_paths: ProjectPaths,
    chat: TypeInputChannel,
    chans_fwd_msg: dict[int, dict],
    anonymiser,
    key_name: str,
    parent_priority,
    lang_detector: LanguageDetector,
    lang_priorities: dict,
    private_chans_priority: int,
    fs: AbstractFileSystem = LOCAL_FS,
):
    forwarded_channels = {}
    for chan_id, m_d in chans_fwd_msg.items():
        fwd_full_chan_d = {}
        m = client.loop.run_until_complete(
            client.get_messages(
                entity=chat, ids=m_d["id"], reply_to=m_d.get("reply_to")
            )
        )
        fwd_from = getattr(m, "fwd_from", None)
        if fwd_from is not None:
            try:
                fwd_id = m.fwd_from.from_id
                new_chan_paths = ChannelPaths(
                    anonymiser.anonymise(fwd_id), project_paths
                )
                new_anon = HMAC_anonymiser(
                    anonymiser.key, save_path=new_chan_paths.anon_map
                )
                _, fwd_full_chan_d = get_full(
                    client,
                    project_paths,
                    anonymiser,
                    key_name,
                    channel=fwd_id,
                    fs=fs,
                )
                new_anon.save_map()
            except ChannelPrivateError:
                # `m.fwd_from.from_id`` is for sure a valid Channel, might be private
                # though. Assign `fwd_full_chan_d` to empty dict in case the channel has
                # actually been made private since last time we collected.
                fwd_full_chan_d = {}
        elif m is not None:
            logger.error("message supposed to have been forwarded is not")
            continue
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


def get_anoned_full_dict(full_chat: ChatFull, anonymiser: HMAC_anonymiser, safe=True):
    channel_save_data = json.loads(full_chat.to_json())
    return anon_full_dict(channel_save_data, anonymiser, safe=safe)


def anon_full_dict(full_dict: dict, anonymiser: HMAC_anonymiser, safe=True):
    anon_func = anonymiser.anonymise
    for c in full_dict["chats"]:
        c["photo"] = None
        c["id"] = anon_func(c["id"], safe=safe)
        c["username"] = anon_func(c["username"], safe=safe)
        c["title"] = anon_func(c["title"], safe=safe)
        if c["usernames"] is not None:
            for un in c["usernames"]:
                un["username"] = anon_func(un["username"], safe=safe)
    full_channel = full_dict["full_chat"]
    full_channel["chat_photo"] = None
    full_channel["id"] = anon_func(full_channel["id"], safe=safe)
    full_channel["linked_chat_id"] = anon_func(
        full_channel["linked_chat_id"], safe=safe
    )
    full_channel["migrated_from_chat_id"] = anon_func(
        full_channel["migrated_from_chat_id"], safe=safe
    )
    if "recommended_channels" in full_dict:
        full_dict["recommended_channels"] = list(
            map(anon_func, full_dict["recommended_channels"])
        )

    def user_anon_func(d):
        return collegram.users.anon_user_d(d, anon_func)

    full_dict["users"] = list(map(user_anon_func, full_dict.get("users", [])))
    if "participants" in full_dict:
        full_dict["participants"] = list(map(user_anon_func, full_dict["participants"]))

    anonymiser.save_map()
    return full_dict


def get_explo_priority(
    fwd_full_chan_d: dict,
    anonymiser,
    parent_priority: int,
    lang_detector: LanguageDetector,
    lang_priorities: dict,
    private_chans_priority: int,
):
    if fwd_full_chan_d:
        full = fwd_full_chan_d["full_chat"]
        chat = get_matching_chat_from_full(fwd_full_chan_d)
        # This gives an overestimate of lifespan since the channel's last query time is
        # necessarily before now, but doesn't matter much here since we want to avoid
        # channels with `updates_per_part_per_day` at a certain order of magnitude.
        lifespan = datetime.datetime.now(
            datetime.UTC
        ) - datetime.datetime.fromisoformat(chat["date"])
        updates_per_part_per_day = full["pts"] / (
            1 + full["participants_count"] * (1 + lifespan.days)
        )
        # We make following test to eliminate channels with huge number of messages and
        # very few participants. Threshold set such that a channel with 100 participants
        # can have up to 10 updates per day.
        if updates_per_part_per_day > 0.1:
            prio = private_chans_priority - 1
        else:
            lang = collegram.text.detect_chan_lang(
                fwd_full_chan_d, anonymiser.inverse_anon_map, lang_detector
            )
            # Some channels may be from a relevant language, but detection was just not
            # conclusive, so default shouldn't be too high.
            lang_prio = lang_priorities.get(lang, 100)
            # lang_prio is both increment and multiplicative factor, thus if some language has
            # prio value N times superior, after exploring N of other language, it'l' be this
            # language's turn.
            prio = parent_priority + lang_prio
    else:
        prio = private_chans_priority
    return prio


def get_extended_save_data(
    client: TelegramClient,
    chat: TypeInputChannel,
    channel_save_data: dict,
    anonymiser,
    project_paths: ProjectPaths,
    key_name: str = "",
    recommended_chans_prios: dict[int, int] | None = None,
    **explo_prio_kwargs,
):
    participants_iter = (
        collegram.users.get_channel_participants(client, chat)
        if channel_save_data["full_chat"].get("can_view_participants", False)
        else []
    )

    channel_save_data["participants"] = [
        json.loads(u.to_json()) for u in participants_iter
    ]

    channel_save_data["recommended_channels"] = []
    for c in get_recommended(client, chat):
        # A priori, this `get_full` call is safe as `GetChannelRecommendationsRequest`
        # should only return public channels, and all these channels should be
        # considered as seen before.
        new_chan_paths = ChannelPaths(anonymiser.anonymise(c.id), project_paths)
        new_anon = HMAC_anonymiser(anonymiser.key, save_path=new_chan_paths.anon_map)
        _, full_chat_d = get_full(
            client,
            project_paths,
            new_anon,
            key_name=key_name,
            channel=c,
        )
        new_anon.save_map()
        if recommended_chans_prios is not None:
            recommended_chans_prios[c.id] = get_explo_priority(
                full_chat_d, anonymiser, **explo_prio_kwargs
            )
        channel_save_data["recommended_channels"].append(c.id)
    anonymiser.save_map()

    for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items():
        count = collegram.messages.get_channel_messages_count(client, chat, f)
        channel_save_data[f"{content_type}_count"] = count

    channel_save_data["last_queried_at"] = datetime.datetime.now(
        datetime.UTC
    ).isoformat()
    return channel_save_data


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
    "recommended_channels": pl.List(pl.Utf8),
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
        if isinstance(last_queried_at, str)
        else last_queried_at
    )
    flat_c["forwards_from"] = c.get("forwards_from")
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
