from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import telethon.sync
from telethon.errors import ChatAdminRequiredError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)


def get_channel_participants(client: TelegramClient, channel_username):
    offset = 0
    limit = 100
    all_participants = []

    while True:
        try:
            participants = client(
                GetParticipantsRequest(
                    channel_username, ChannelParticipantsSearch(""), offset, limit, hash=0
                )
            )
        except ChatAdminRequiredError:
            logger.warning(f"No access to participants of {channel_username}")

        if not participants.users:
            break

        all_participants.extend(participants.users)
        offset += len(participants.users)

    return all_participants


def get_channel_users_dict(client: TelegramClient, channel_username, f_users=None):
    users_dict = {}
    participants = get_channel_participants(client, channel_username)
    for p in participants:
        # Users only have a username if they have set one manually, so we get the ID as
        # reliable identifier.
        users_dict[p.id] = p.to_dict()
        users_dict[p.id]["user_object"] = p
        users_dict[p.id]['channel_username'] = channel_username
    return users_dict
