from __future__ import annotations

import datetime
import inspect
import logging
from typing import TYPE_CHECKING

from telethon.helpers import add_surrogate, del_surrogate
from telethon.tl.functions.messages import GetRepliesRequest
from telethon.tl.types import (
    Message,
    MessageActionChannelCreate,
    MessageActionChannelMigrateFrom,
    MessageActionChatAddUser,
    MessageActionChatCreate,
    MessageActionChatDeleteUser,
    MessageActionChatEditPhoto,
    MessageActionChatEditTitle,
    MessageActionChatJoinedByLink,
    MessageActionChatMigrateTo,
    MessageEntityEmail,
    MessageEntityMention,  # for channels
    MessageEntityMentionName,  # for users (has a `user_id` attr)
    MessageEntityTextUrl,  # has a `url` attr
    MessageEntityUrl,
    MessageService,
    PeerChannel,
    PeerChat,
    PeerUser,
)

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel

logger = logging.getLogger(__name__)


def get_channel_messages(client: TelegramClient, channel: str | Channel, dt_from: datetime.datetime, dt_to: datetime.datetime, anon_func):
    '''
    dt_to exclusive
    '''
    offset_id = 0
    limit = 10000
    all_messages = []
    total_messages = 0
    keep_going = True

    while keep_going:
        chunk_messages = []
        logger.info(f"Current Offset ID is: {offset_id}; Total Messages: {total_messages}")
        # Splitting by 10k chunks in case of disconnection, to at least save something (TODO: save)
        # Telethon docs are misleading, `offset_date` is in fact a datetime.
        messages = client.iter_messages(
            entity=channel,
            offset_date=dt_to,
            offset_id=offset_id,
            limit=limit,
        )

        for message in messages:
            # Take messages in until we've gone further than `date_until` in the past
            # (works because HistoryRequest gets messages in reverse chronological order
            # by default)
            if message.date >= dt_from:
                chunk_messages.append(preprocess(message, anon_func))
                if getattr(message, "replies", None) is not None and message.replies.comments:
                    chunk_messages.extend(get_comments(client, channel, message.id, anon_func))
            else:
                keep_going = False
                break

        if len(chunk_messages) < limit:
            # Needed when reaching first message ever posted
            keep_going = False

        offset_id = message.id
        all_messages.extend(chunk_messages)
        total_messages = len(all_messages)

    return all_messages


def get_comments(client: TelegramClient, channel: str | Channel, message_id, anon_func)-> list[ExtendedMessage]:
    result = client(GetRepliesRequest(
        peer=channel,
        msg_id=message_id,
        offset_id=0,
        offset_date=datetime.datetime.now(),
        add_offset=0,
        limit=-1,
        max_id=0,
        min_id=0,
        hash=0
    ))

    comments = []
    for m in result.messages:
        preprocessed_m = preprocess(m, anon_func)
        preprocessed_m.comments_msg_id = message_id
        comments.append(preprocessed_m)
    return comments


def preprocess(message: Message | MessageService, anon_func) -> ExtendedMessage:
    preproced_message = message # TODO: copy?
    if isinstance(message, Message):
        preproced_message = ExtendedMessage.from_message(preproced_message)
        preproced_message = preprocess_entities(preproced_message, anon_func)
    preproced_message = anonymise_metadata(preproced_message, anon_func)
    return preproced_message


def preprocess_entities(message: ExtendedMessage, anon_func) -> ExtendedMessage:
    anon_message = message # TODO: copy?
    surr_text = add_surrogate(message.message)

    if message.entities is not None:
        anon_subs = [(0, 0, "")]
        for e in message.entities:
            e_start = e.offset
            e_end = e.offset + e.length
            if isinstance(e, (MessageEntityMention, MessageEntityMentionName)):
                anon_mention = anon_func(surr_text[e_start + 1: e_end])
                anon_message.text_mentions.add(anon_mention)
                anon_subs.append((e_start + 1, e_end, anon_mention))
            elif isinstance(e, MessageEntityEmail):
                email = surr_text[e_start: e_end]
                # Keep the email format to be able to identify this as an email later on.
                anon_email = '@'.join([anon_func(part) for part in email.split("@")])
                anon_subs.append((e_start, e_end, anon_email))
            elif isinstance(e, MessageEntityUrl):
                anon_message.text_urls.add(surr_text[e_start: e_end])
            elif isinstance(e, MessageEntityTextUrl):
                anon_message.text_urls.add(e.url)

        if len(anon_subs) > 1:
            anon_message.text = del_surrogate(
                ''.join([
                    surr_text[anon_subs[i][1]: anon_subs[i+1][0]] + anon_subs[i+1][2]
                    for i in range(len(anon_subs)-1)
                ])
                + surr_text[anon_subs[-1][1]:]
            )
    return anon_message


def anonymise_metadata(message: ExtendedMessage | MessageService, anon_func):
    message = anonymise_peer(message, "peer_id", anon_func)
    message = anonymise_peer(message, "from_id", anon_func)

    if isinstance(message, ExtendedMessage):
        message.post_author = anon_func(message.post_author)
        message = anonymise_peer(message, "reply_to.reply_to_peer_id", anon_func)

        if message.replies is not None:
            message.replies.channel_id = anon_func(message.replies.channel_id)
            if message.replies.recent_repliers is not None:
                for u in message.replies.recent_repliers:
                    u.user_id = anon_func(u.user_id)

        if message.fwd_from is not None:
            message.raw_fwd_from_channel_id = getattr(message.fwd_from.from_id, 'channel_id', None)
            message = anonymise_peer(message, "fwd_from.from_id", anon_func)
            message.fwd_from.from_name = anon_func(message.fwd_from.from_name)
            message.fwd_from.post_author = anon_func(message.fwd_from.post_author)

    elif isinstance(message, MessageService):
        message.action = anonymise_peer(message.action, "peer", anon_func)
        message.action = anonymise_peer(message.action, "peer_id", anon_func)
        if isinstance(message.action, (MessageActionChatAddUser, MessageActionChatCreate)):
            message.action.users = [anon_func(uid) for uid in message.action.users]
        elif isinstance(message.action, MessageActionChatDeleteUser):
            message.action.user_id = anon_func(message.action.user_id)
        elif isinstance(message.action, MessageActionChatJoinedByLink):
            message.action.inviter_id = anon_func(message.action.inviter_id)
        elif isinstance(message.action, MessageActionChatEditPhoto):
            message.action.photo.id = anon_func(message.action.photo.id)
        elif isinstance(message.action, MessageActionChatMigrateTo):
            message.action.channel_id = anon_func(message.action.channel_id)

        actions_with_title = (MessageActionChannelCreate, MessageActionChannelMigrateFrom, MessageActionChatCreate, MessageActionChatEditTitle)
        if isinstance(message.action, actions_with_title):
            message.action.title = anon_func(message.action.title)
            if isinstance(message.action, MessageActionChannelMigrateFrom):
                message.action.chat_id = anon_func(message.action.chat_id)
    return message


def anonymise_peer(object, path_to_peer, anon_func):
    path_parts = path_to_peer.split('.')
    peer_obj = getattr(object, path_parts[0], None)
    for i in range(1, len(path_parts)-1):
        if peer_obj is not None:
            peer_obj = getattr(peer_obj, path_parts[i], None)

    if peer_obj is not None:
        if isinstance(peer_obj, PeerChannel):
            setattr(peer_obj, "channel_id", anon_func(peer_obj.channel_id))
        elif isinstance(peer_obj, PeerUser):
            setattr(peer_obj, "user_id", anon_func(peer_obj.user_id))
        elif isinstance(peer_obj, PeerChat):
            setattr(peer_obj, "chat_id", anon_func(peer_obj.chat_id))
    return object


# First is self, so take from index 1 on.
MESSAGE_INIT_ARGS = inspect.getfullargspec(Message).args[1:]

class ExtendedMessage(Message):
    # Created this class because m.reply_msg_id did not match the commented-on message's
    # id, so need to save the info somehow.

    @classmethod
    def from_message(
        cls, message: Message, comments_msg_id: int | None = None,
        text_urls: set[str] | None = None, text_mentions: set[str] | None = None,
        raw_fwd_from_channel_id: int | None = None,
    ):
        instance = cls(*[getattr(message, a) for a in MESSAGE_INIT_ARGS])
        instance.comments_msg_id = comments_msg_id
        instance.text_urls = set() if text_urls is None else text_urls
        instance.text_mentions = set() if text_mentions is None else text_mentions
        instance.raw_fwd_from_channel_id = raw_fwd_from_channel_id
        return instance

    def to_dict(self):
        # Anything that is added here will be saved, as this is called by `to_json`
        d = super().to_dict()
        d['comments_msg_id'] = self.comments_msg_id
        # Cast to list for JSON serialisation:
        d['text_urls'] = list(self.text_urls)
        d['text_mentions'] = list(self.text_mentions)
        return d
