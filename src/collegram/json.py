from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Optional, Union

import msgspec

from collegram.utils import LOCAL_FS

if TYPE_CHECKING:
    from pathlib import Path

    from fsspec import AbstractFileSystem


RELEVANT_MEDIA_TYPES = {
    "MessageMediaWebPage": "webpage",
    "MessageMediaPhoto": "photo",
    "MessageMediaDocument": "document",
}
PEER_TYPES_ID = {
    "PeerChannel": "channel_id",
    "PeerUser": "user_id",
    "PeerChat": "chat_id",
}


class MessageBase(msgspec.Struct, tag_field="_"):
    id: int
    date: Optional[datetime.datetime]


class MessageService(MessageBase):
    action: Any


class Action(msgspec.Struct):
    pass  # TODO?


class MaybeForwardedMessage(msgspec.Struct):
    id: int
    fwd_from: Optional[FwdFrom] = None
    reply_to: Optional[ReplyHeader] = None


class Message(MessageBase):
    message: str
    mentioned: Optional[bool] = None
    legacy: Optional[bool] = None
    out: Optional[bool] = None
    media_unread: Optional[bool] = None
    silent: Optional[bool] = None
    noforwards: Optional[bool] = None
    post: Optional[bool] = None
    from_scheduled: Optional[bool] = None
    edit_hide: Optional[bool] = None
    pinned: Optional[bool] = None
    invert_media: Optional[bool] = None
    via_bot_id: Optional[int] = None
    views: Optional[int] = None
    forwards: Optional[int] = None
    edit_date: Optional[datetime.datetime] = None
    reactions: Optional[Reactions] = None
    from_id: Optional[Peer] = None
    media: Optional[MessageMediaTypes] = None
    fwd_from: Optional[FwdFrom] = None
    replies: Optional[Replies] = None
    reply_to: Optional[ReplyHeader] = None
    text_urls: Optional[list[str]] = None
    text_mentions: Optional[list[str]] = None


class Peer(msgspec.Struct):
    _: str
    channel_id: Optional[str] = None
    user_id: Optional[str] = None
    chat_id: Optional[str] = None


class MessageMediaBase(msgspec.Struct, tag_field="_"):
    pass


class MessageMediaPhoto(MessageMediaBase):
    photo: MediaType


class MessageMediaDocument(MessageMediaBase):
    document: MediaType
    video: Optional[bool] = None
    voice: Optional[bool] = None


class MessageMediaWebPage(MessageMediaBase):
    webpage: MediaType


ignored_media_structs = [
    msgspec.defstruct(f"MessageMedia{name}", [], bases=(MessageMediaBase,))
    for name in (
        "Geo",
        "Contact",
        "Unsupported",
        "Venue",
        "Game",
        "Invoice",
        "GeoLive",
        "Poll",
        "Dice",
        "Story",
        "Giveaway",
        "GiveawayResults",
    )
]

MessageMediaTypes = Union[
    tuple(
        [MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage]
        + ignored_media_structs
    )
]


class MediaType(msgspec.Struct):
    id: int


class FwdFrom(msgspec.Struct):
    date: Optional[datetime.datetime]
    from_id: Optional[Peer] = None
    channel_post: Optional[int] = None


class Replies(msgspec.Struct):
    replies: int
    comments: Optional[bool] = None


class ReplyHeader(msgspec.Struct):
    reply_to_top_id: Optional[int] = None
    reply_to_msg_id: Optional[int] = None
    reply_to_peer_id: Optional[Peer] = None
    forum_topic: Optional[bool] = None


class Reactions(msgspec.Struct):
    results: Optional[list[ReactionCount]] = None


class ReactionCount(msgspec.Struct):
    count: int
    reaction: Reaction


class Reaction(msgspec.Struct):
    emoticon: Optional[str] = None
    document_id: Optional[int] = None


MessageJSONDecodeType = Union[Message, MessageService]
MESSAGE_JSON_DECODER = msgspec.json.Decoder(type=MessageJSONDecodeType)
FAST_FORWARD_DECODER = msgspec.json.Decoder(type=MaybeForwardedMessage)


def read_messages_json(
    path: str | Path,
    fs: AbstractFileSystem = LOCAL_FS,
    decoder: msgspec.json.Decoder = MESSAGE_JSON_DECODER,
):
    with fs.open(str(path), "r") as f:
        return decoder.decode_lines(f.read())


def read_message(
    message: bytes | str, decoder: msgspec.json.Decoder = MESSAGE_JSON_DECODER
):
    return decoder.decode(message)


def yield_message(
    fpath: str | Path,
    fs: AbstractFileSystem = LOCAL_FS,
    decoder: msgspec.json.Decoder = MESSAGE_JSON_DECODER,
):
    with fs.open(str(fpath), "r") as f:
        for line in f:
            if line.strip("\n"):
                yield read_message(line, decoder)
