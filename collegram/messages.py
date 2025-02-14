from __future__ import annotations

import datetime
import inspect
import logging
import re
from typing import TYPE_CHECKING

from telethon.errors import MsgIdInvalidError
from telethon.helpers import add_surrogate
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import (
    Document,
    InputMessagesFilterDocument,
    InputMessagesFilterEmpty,
    InputMessagesFilterGif,
    InputMessagesFilterMusic,
    InputMessagesFilterPhotos,
    InputMessagesFilterUrl,
    InputMessagesFilterVideo,
    InputMessagesFilterVoice,
    Message,
    MessageActionChannelCreate,
    MessageActionChannelMigrateFrom,
    MessageActionChatAddUser,
    MessageActionChatCreate,
    MessageActionChatDeleteUser,
    MessageActionChatEditPhoto,
    MessageActionChatEditTitle,
    MessageActionChatJoinedByLink,
    MessageEntityEmail,
    MessageEntityMention,  # for channels
    MessageEntityMentionName,  # for users (has a `user_id` attr)
    MessageEntityTextUrl,  # has a `url` attr
    MessageEntityUrl,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
    MessageReplyHeader,
    MessageService,
    PeerUser,
    Photo,
    ReactionCustomEmoji,
    ReactionEmoji,
    WebPage,
    WebPageNotModified,
)
from telethon.tl.types.messages import ChannelMessages

import collegram.json
import collegram.media
from collegram.utils import LOCAL_FS

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Iterable

    from fsspec import AbstractFileSystem
    from telethon import TelegramClient
    from telethon.tl.types import (
        TypeInputChannel,
        TypeMessagesFilter,
        TypePeer,
    )

logger = logging.getLogger(__name__)


MESSAGE_CONTENT_TYPE_MAP = {
    "document": InputMessagesFilterDocument(),
    "message": InputMessagesFilterEmpty(),
    "gif": InputMessagesFilterGif(),
    "music": InputMessagesFilterMusic(),
    "photo": InputMessagesFilterPhotos(),
    "url": InputMessagesFilterUrl(),
    "video": InputMessagesFilterVideo(),
    "voice": InputMessagesFilterVoice(),
}


def get_comments_iter(
    client: TelegramClient,
    channel: TypeInputChannel,
    message_id: int,
) -> Iterable[Message]:
    try:
        return client.iter_messages(channel, reply_to=message_id)
    except MsgIdInvalidError:
        logger.error(f"no replies found for message ID {message_id}")
        breakpoint()
        return []


def yield_comments(
    client: TelegramClient,
    channel: TypeInputChannel,
    message: Message,
):
    replies = getattr(message, "replies", None)
    if replies is not None and replies.replies > 0 and replies.comments:
        for c in get_comments_iter(client, channel, message.id):
            yield c


async def save_channel_messages(
    client: TelegramClient,
    channel: TypeInputChannel,
    dt_from: datetime.datetime,
    dt_to: datetime.datetime,
    forwards_stats: dict[int, dict],
    linked_chans_stats: dict[str, dict],
    anon_func,
    messages_save_path,
    media_save_path: Path,
    offset_id=0,
    fs: AbstractFileSystem = LOCAL_FS,
):
    """
    offset_id: messages with ID superior to `offset_id` will be retrieved
    """
    with fs.open(messages_save_path, "a") as f:
        async for m in yield_channel_messages(
            client,
            channel,
            dt_from,
            dt_to,
            forwards_stats,
            linked_chans_stats,
            anon_func,
            media_save_path,
            offset_id=offset_id,
            fs=fs,
        ):
            f.write(m.to_json())
            f.write("\n")


async def yield_channel_messages(
    client: TelegramClient,
    channel: TypeInputChannel,
    dt_from: datetime.datetime,
    dt_to: datetime.datetime,
    forwards_stats: dict[int, dict],
    linked_chans_stats: dict[str, dict],
    anon_func,
    media_save_path: Path,
    offset_id=0,
    fs: AbstractFileSystem = LOCAL_FS,
):
    # Telethon docs are misleading, `offset_date` is in fact a datetime.
    async for message in client.iter_messages(
        entity=channel,
        offset_date=dt_from,
        offset_id=offset_id,
        reverse=True,
    ):
        # Take messages in until we've reached `dt_to` (works because
        # `iter_messages` gets messages in reverse chronological order by default,
        # and we reversed it)
        if message.date <= dt_to:
            preprocessed_m = preprocess(
                message,
                forwards_stats,
                linked_chans_stats,
                anon_func,
                media_save_path,
                fs=fs,
            )
            yield preprocessed_m
        else:
            break


def query_channel_messages(
    client: TelegramClient,
    channel: TypeInputChannel,
    f: TypeMessagesFilter,
    query: str = "",
) -> ChannelMessages:
    return client.loop.run_until_complete(
        client(SearchRequest(channel, query, f, None, None, 0, 0, 0, 0, 0, 0))
    )


def get_channel_messages_count(
    client: TelegramClient,
    channel: TypeInputChannel,
    f: TypeMessagesFilter,
    query: str = "",
) -> int:
    return query_channel_messages(client, channel, f, query=query).count


def preprocess(
    message: Message | MessageService,
    forwards_stats: dict[int, dict],
    linked_chans_stats: dict[str, dict],
    anon_func,
    media_save_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
) -> ExtendedMessage | MessageService:
    preproced_message = message  # TODO: copy?
    if isinstance(message, Message):
        preproced_message = ExtendedMessage.from_message(preproced_message)
        preproced_message = preprocess_entities(
            preproced_message, linked_chans_stats, anon_func
        )
        if preproced_message.media is not None:
            _ = collegram.media.preprocess(
                preproced_message.media,
                media_save_path,
                fs=fs,
            )
    preproced_message = anonymise_metadata(preproced_message, forwards_stats, anon_func)
    return preproced_message


def del_surrogate(text):
    return text.encode("utf-16", "surrogatepass").decode("utf-16", "surrogatepass")


def preprocess_entities(
    message: ExtendedMessage, linked_chans_stats: dict[str, dict], anon_func
) -> ExtendedMessage:
    anon_message = message  # TODO: copy?
    surr_text = add_surrogate(message.message)
    msg_linked_chans = set()

    if message.entities is not None:
        anon_subs = [(0, 0, "")]
        entities = []
        for e in message.entities:
            e_start = e.offset
            e_end = e.offset + e.length
            if isinstance(e, (MessageEntityMention, MessageEntityMentionName)):
                # A MessageEntityMention starts with an "@", which we keep as is.
                start = e_start + 1 * int(isinstance(e, MessageEntityMention))
                anon_mention = anon_func(del_surrogate(surr_text[start:e_end]))
                anon_message.text_mentions.add(anon_mention)
                anon_subs.append((start, e_end, anon_mention))
            elif isinstance(e, MessageEntityEmail):
                email = del_surrogate(surr_text[e_start:e_end])
                # Keep the email format to be able to identify this as an email later on.
                anon_email = "@".join([anon_func(part) for part in email.split("@")])
                anon_subs.append((e_start, e_end, anon_email))
            elif isinstance(e, (MessageEntityUrl, MessageEntityTextUrl)):
                url = (
                    e.url
                    if isinstance(e, MessageEntityTextUrl)
                    else del_surrogate(surr_text[e_start:e_end])
                )
                username_match = re.match(r"(https://t\.me/)(\w+)(.*)", url)
                if username_match is None:
                    anon_message.text_urls.add(url)
                else:
                    un = username_match.group(2)
                    prev_len = len(msg_linked_chans)
                    msg_linked_chans.add(un)
                    if len(msg_linked_chans) > prev_len:
                        linked_stats = linked_chans_stats.get(un, {})
                        linked_stats["nr_messages"] = (
                            linked_stats.get("nr_messages", 0) + 1
                        )
                        linked_stats["first_message_date"] = linked_stats.get(
                            "first_message_date", message.date
                        )
                        linked_stats["last_message_date"] = message.date
                        linked_chans_stats[un] = linked_stats
                    anon_url = username_match.expand(
                        r"\g<1>{}\g<3>".format(anon_func(un))
                    )
                    anon_message.text_urls.add(anon_url)
                    if isinstance(e, MessageEntityTextUrl):
                        e.url = anon_url
                    anon_subs.append((e_start, e_end, anon_url))

            # For some reason, message.entities gets emptied as we make changes above,
            # hence why we reassign the list at the end here.
            entities.append(e)
        anon_message.entities = entities

        if len(anon_subs) > 1:
            anon_message.message = del_surrogate(
                "".join(
                    [
                        surr_text[anon_subs[i][1] : anon_subs[i + 1][0]]
                        + anon_subs[i + 1][2]
                        for i in range(len(anon_subs) - 1)
                    ]
                )
                + surr_text[anon_subs[-1][1] :]
            )
    return anon_message


def anonymise_metadata(
    message: ExtendedMessage | MessageService,
    forwards_stats: dict[int, dict],
    anon_func,
):
    message = anonymise_opt_peer(message, "peer_id", anon_func)
    message = anonymise_opt_peer(message, "from_id", anon_func)

    if isinstance(message, ExtendedMessage):
        message.post_author = anon_func(message.post_author)
        message.reply_to = anonymise_opt_peer(
            message.reply_to, "reply_to_peer_id", anon_func
        )

        if message.replies is not None:
            if message.replies.recent_repliers is not None:
                for r in message.replies.recent_repliers:
                    # Repliers are not necessarily users, can be a channel.
                    r = anonymise_peer(r, anon_func)

        if message.fwd_from is not None:
            fwd_from_channel_id = getattr(message.fwd_from.from_id, "channel_id", None)
            if fwd_from_channel_id is not None:
                fwd_stats = forwards_stats.get(fwd_from_channel_id, {})
                fwd_stats["nr_messages"] = fwd_stats.get("nr_messages", 0) + 1
                fwd_stats["first_message_date"] = fwd_stats.get(
                    "first_message_date", message.date
                )
                fwd_stats["last_message_date"] = message.date
                forwards_stats[fwd_from_channel_id] = fwd_stats
            message.fwd_from = anonymise_opt_peer(
                message.fwd_from, "from_id", anon_func
            )
            message.fwd_from = anonymise_opt_peer(
                message.fwd_from, "saved_from_peer", anon_func
            )
            message.fwd_from.from_name = anon_func(message.fwd_from.from_name)
            message.fwd_from.post_author = anon_func(message.fwd_from.post_author)

    elif isinstance(message, MessageService):
        message.action = anonymise_opt_peer(message.action, "peer", anon_func)
        message.action = anonymise_opt_peer(message.action, "peer_id", anon_func)
        if isinstance(
            message.action, (MessageActionChatAddUser, MessageActionChatCreate)
        ):
            message.action.users = [anon_func(uid) for uid in message.action.users]
        elif isinstance(message.action, MessageActionChatDeleteUser):
            message.action.user_id = anon_func(message.action.user_id)
        elif isinstance(message.action, MessageActionChatJoinedByLink):
            message.action.inviter_id = anon_func(message.action.inviter_id)
        elif isinstance(message.action, MessageActionChatEditPhoto):
            message.action.photo.id = anon_func(message.action.photo.id)

        actions_with_title = (
            MessageActionChannelCreate,
            MessageActionChannelMigrateFrom,
            MessageActionChatCreate,
            MessageActionChatEditTitle,
        )
        if isinstance(message.action, actions_with_title):
            message.action.title = anon_func(message.action.title)
            if isinstance(message.action, MessageActionChannelMigrateFrom):
                message.action.chat_id = anon_func(message.action.chat_id)
    return message


def anonymise_opt_peer(object, path_to_peer, anon_func):
    # TODO: fix for path with parts?
    path_parts = path_to_peer.split(".")
    peer_obj = getattr(object, path_parts[0], None)
    for i in range(1, len(path_parts) - 1):
        if peer_obj is not None:
            peer_obj = getattr(peer_obj, path_parts[i], None)

    if peer_obj is not None:
        peer_obj = anonymise_peer(peer_obj, anon_func)
    return object


def anonymise_peer(obj: TypePeer, anon_func):
    if isinstance(obj, PeerUser):
        setattr(obj, "user_id", anon_func(obj.user_id))
    return obj


# First is self, so take from index 1 on.
MESSAGE_INIT_ARGS = inspect.getfullargspec(Message).args[1:]


class ExtendedMessage(Message):
    # Created this class because m.reply_msg_id did not match the commented-on message's
    # id, so need to save the info somehow.

    @classmethod
    def from_message(
        cls,
        message: Message,
        text_urls: set[str] | None = None,
        text_mentions: set[str] | None = None,
    ):
        instance = cls(*[getattr(message, a) for a in MESSAGE_INIT_ARGS])
        instance.text_urls = set() if text_urls is None else text_urls
        instance.text_mentions = set() if text_mentions is None else text_mentions
        return instance

    def to_dict(self):
        # Anything that is added here will be saved, as this is called by `to_json`
        d = super().to_dict()
        # Cast to list for JSON serialisation:
        d["text_urls"] = list(self.text_urls)
        d["text_mentions"] = list(self.text_mentions)
        return d


def to_flat_dict(m: ExtendedMessage):
    m_dict = m.to_dict()
    return flatten_dict(m, m_dict)


def flatten_dict(m: ExtendedMessage, m_dict: dict):
    for field in collegram.json.NEW_MSG_FIELDS.keys():
        m_dict[field] = None
    for field in collegram.json.DISCARDED_MSG_FIELDS:
        m_dict.pop(field)

    media = m.media
    if media is not None:
        # TODO: save media separately? like whole JSON / parquets of photos / videos
        # / web pages / documents
        if isinstance(media, MessageMediaWebPage):
            m_dict["media_type"] = "webpage"
            if not isinstance(media.webpage, WebPageNotModified):
                m_dict["media_id"] = media.webpage.id
                m_dict["webpage_preview_url"] = media.webpage.url
            if isinstance(media.webpage, WebPage):
                m_dict["webpage_preview_type"] = media.webpage.type
                m_dict["webpage_preview_site_name"] = media.webpage.site_name
                m_dict["webpage_preview_title"] = media.webpage.title
                m_dict["webpage_preview_description"] = media.webpage.description
        elif isinstance(media, MessageMediaPhoto):
            m_dict["media_type"] = "photo"
            if isinstance(media.photo, Photo):
                m_dict["media_id"] = media.photo.id
        elif isinstance(media, MessageMediaDocument):
            if media.video:
                m_dict["media_type"] = "video"
            elif media.voice:
                m_dict["media_type"] = "voice"
            else:
                m_dict["media_type"] = "document"
            if isinstance(media.document, Document):
                m_dict["media_id"] = media.document.id
        else:
            m_dict["media_type"] = "other"

    from_id = m.from_id
    if from_id is not None:
        from_type = type(from_id).__name__
        m_dict["from_type"] = from_type
        m_dict["from_id"] = getattr(from_id, collegram.json.PEER_TYPES_ID[from_type])

    peer_id = m.peer_id
    if peer_id is not None:
        peer_type = type(peer_id).__name__
        m_dict["peer_type"] = peer_type
        m_dict["peer_id"] = getattr(peer_id, collegram.json.PEER_TYPES_ID[peer_type])

    reply_to = m.reply_to
    if isinstance(reply_to, MessageReplyHeader):
        m_dict["replies_to_msg_id"] = reply_to.reply_to_msg_id
        m_dict["replies_to_thread_msg_id"] = reply_to.reply_to_top_id
        m_dict["replies_to_chan_id"] = getattr(
            reply_to.reply_to_peer_id, "channel_id", None
        )

    fwd_from = m.fwd_from
    if fwd_from is not None:
        m_dict["fwd_from_date"] = fwd_from.date
        m_dict["fwd_from_msg_id"] = fwd_from.channel_post
        if fwd_from.from_id is not None:
            fwd_from_type = type(fwd_from.from_id).__name__
            m_dict["fwd_from_type"] = fwd_from_type
            m_dict["fwd_from_id"] = getattr(
                fwd_from.from_id, collegram.json.PEER_TYPES_ID[fwd_from_type]
            )

    replies = m.replies
    if replies is not None:
        m_dict["nr_replies"] = replies.replies
        m_dict["has_comments"] = replies.comments
    else:
        m_dict["nr_replies"] = 0
        m_dict["has_comments"] = False

    if m.reactions is not None:
        # There can be big number of different reactions, so keep this as dict
        # (converted to struct by Polars).
        reaction_d = {}
        if m.reactions.results is not None:
            for r in m.reactions.results:
                if isinstance(r.reaction, ReactionEmoji):
                    key = r.reaction.emoticon
                elif isinstance(r.reaction, ReactionCustomEmoji):
                    # Cast `document_id` to string to have consistent type.
                    key = str(r.reaction.document_id)
                else:
                    continue
                reaction_d[key] = r.count
        m_dict["reactions"] = reaction_d
    return m_dict
