from __future__ import annotations

import datetime
import inspect
from typing import TYPE_CHECKING, Any, Optional, Union

import msgspec
import polars as pl

from collegram.utils import LOCAL_FS, py_to_pl_types

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
    comments_msg_id: Optional[int] = None  # DEPRECATED
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


class MediaType(msgspec.Struct):
    id: int


class MessageMediaWebPage(MessageMediaBase):
    webpage: WebPagePreview


class WebPagePreview(msgspec.Struct):
    id: int
    url: Optional[str] = None
    type: Optional[str] = None
    site_name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None


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


class FwdFrom(msgspec.Struct):
    date: Optional[datetime.datetime]
    from_id: Optional[Peer] = None
    channel_post: Optional[int] = None


class Replies(msgspec.Struct):
    replies: int
    comments: Optional[bool] = None


class ReplyHeader(msgspec.Struct):
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


NEW_MSG_FIELDS = {
    "media_type": pl.Utf8,
    "media_id": pl.Int64,
    "webpage_preview_url": pl.Utf8,
    "webpage_preview_type": pl.Utf8,
    "webpage_preview_site_name": pl.Utf8,
    "webpage_preview_title": pl.Utf8,
    "webpage_preview_description": pl.Utf8,
    "from_type": pl.Utf8,
    "from_id": pl.Utf8,
    "replies_to_msg_id": pl.Int64,
    "replies_to_chan_id": pl.Utf8,
    "fwd_from_date": pl.Datetime(time_zone="UTC"),
    "fwd_from_type": pl.Utf8,
    "fwd_from_id": pl.Utf8,
    "fwd_from_msg_id": pl.Int64,
    "nr_replies": pl.Int64,
    "has_comments": pl.Boolean,
}
CHANGED_MSG_FIELDS = {
    "reactions": pl.Struct,
}
DISCARDED_MSG_FIELDS = (
    "media",
    "from_id",
    "reply_to",
    "fwd_from",
    "replies",
    "reactions",
)


def get_pl_schema():
    annots = inspect.getfullargspec(Message).annotations
    discarded_args = DISCARDED_MSG_FIELDS
    msg_schema = {
        arg: py_to_pl_types(annots[arg])
        for arg in set(annots.keys()).difference(discarded_args)
    }
    msg_schema = {**msg_schema, **CHANGED_MSG_FIELDS, **NEW_MSG_FIELDS}
    return msg_schema


def messages_to_dict(messages: list[Message]):
    # can also determine nested from Message.__annotations__, but not super robust
    non_nested_f = set(Message.__struct_fields__).difference(DISCARDED_MSG_FIELDS)
    final_fields = non_nested_f.union(NEW_MSG_FIELDS.keys()).union(
        CHANGED_MSG_FIELDS.keys()
    )
    m_dict = {field: [] for field in final_fields}
    for m in messages:
        for field in non_nested_f:
            m_dict[field].append(getattr(m, field))

        media = m.media
        if media is not None:
            # TODO: save media separately? like whole JSON / parquets of photos / videos
            # / web pages / documents
            if isinstance(media, MessageMediaWebPage):
                m_dict["media_type"].append("webpage")
                m_dict["media_id"].append(media.webpage.id)
                m_dict["webpage_preview_url"].append(media.webpage.url)
                m_dict["webpage_preview_type"].append(media.webpage.type)
                m_dict["webpage_preview_site_name"].append(media.webpage.site_name)
                m_dict["webpage_preview_title"].append(media.webpage.title)
                m_dict["webpage_preview_description"].append(media.webpage.description)
            else:
                if isinstance(media, MessageMediaPhoto):
                    m_dict["media_type"].append("photo")
                    m_dict["media_id"].append(media.photo.id)
                elif isinstance(media, MessageMediaDocument):
                    if media.video:
                        m_dict["media_type"].append("video")
                    elif media.voice:
                        m_dict["media_type"].append("voice")
                    else:
                        m_dict["media_type"].append("document")
                    m_dict["media_id"].append(media.document.id)
                else:
                    m_dict["media_type"].append("other")
                    m_dict["media_id"].append(None)
                m_dict["webpage_preview_url"].append(None)
                m_dict["webpage_preview_type"].append(None)
                m_dict["webpage_preview_site_name"].append(None)
                m_dict["webpage_preview_title"].append(None)
                m_dict["webpage_preview_description"].append(None)
        else:
            m_dict["media_type"].append(None)
            m_dict["media_id"].append(None)
            m_dict["webpage_preview_url"].append(None)
            m_dict["webpage_preview_type"].append(None)
            m_dict["webpage_preview_site_name"].append(None)
            m_dict["webpage_preview_title"].append(None)
            m_dict["webpage_preview_description"].append(None)

        from_id = m.from_id
        if from_id is not None:
            m_dict["from_type"].append(from_id._)
            m_dict["from_id"].append(getattr(from_id, PEER_TYPES_ID[from_id._]))
        else:
            m_dict["from_id"].append(None)
            m_dict["from_type"].append(None)

        reply_to = m.reply_to
        m_dict["replies_to_msg_id"].append(getattr(reply_to, "reply_to_msg_id", None))
        m_dict["replies_to_chan_id"].append(
            None
            if reply_to is None
            else getattr(reply_to.reply_to_peer_id, "channel_id", None)
        )

        fwd_from = m.fwd_from
        if fwd_from is not None:
            m_dict["fwd_from_date"].append(fwd_from.date)
            m_dict["fwd_from_msg_id"].append(fwd_from.channel_post)
            if fwd_from.from_id is not None:
                m_dict["fwd_from_type"].append(fwd_from.from_id._)
                m_dict["fwd_from_id"].append(
                    getattr(fwd_from.from_id, PEER_TYPES_ID[fwd_from.from_id._])
                )
            else:
                m_dict["fwd_from_type"].append(None)
                m_dict["fwd_from_id"].append(None)
        else:
            m_dict["fwd_from_date"].append(None)
            m_dict["fwd_from_msg_id"].append(None)
            m_dict["fwd_from_type"].append(None)
            m_dict["fwd_from_id"].append(None)

        replies = m.replies
        if replies is not None:
            m_dict["nr_replies"].append(replies.replies)
            m_dict["has_comments"].append(replies.comments)
        else:
            m_dict["nr_replies"].append(0)
            m_dict["has_comments"].append(False)

        if m.reactions is not None:
            # There can be big number of different reactions, so keep this as dict
            # (converted to struct by Polars).
            reaction_d = {}
            if m.reactions.results is not None:
                for r in m.reactions.results:
                    # Cast `document_id` to string to have consistent type.
                    key = r.reaction.emoticon or str(r.reaction.document_id)
                    reaction_d[key] = r.count
            m_dict["reactions"].append(reaction_d)
        else:
            m_dict["reactions"].append(None)
    return m_dict


def service_messages_to_dict(messages: list[MessageService]):
    nested_f = ["action"]
    non_nested_f = set(MessageService.__struct_fields__).difference(nested_f)
    new_f = ["action", "action_type"]
    final_fields = non_nested_f.union(new_f)
    m_dict = {field: [] for field in final_fields}
    for m in messages:
        for field in non_nested_f:
            m_dict[field].append(getattr(m, field))
        action_d = msgspec.to_builtins(m.action)
        m_dict["action_type"].append(action_d.pop("_"))
        m_dict["action"].append(action_d)
    return m_dict
