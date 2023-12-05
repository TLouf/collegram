from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import telethon.sync
from telethon.errors import ChatAdminRequiredError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel, User

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
            break

        if not participants.users:
            break

        all_participants.extend(participants.users)
        offset += len(participants.users)

    return all_participants


def get_channel_users(client: TelegramClient, channel: str | Channel, anon_func) -> list[User]:
    try:
        participants = client.iter_participants(channel)
    except ChatAdminRequiredError:
        logger.warning(f"No access to participants of {channel}")
        participants = []

    users = []
    for p in participants:
        # We completely anonymise the following fields:
        for field in ("first_name", "last_name", "username", "phone", "photo"):
            setattr(p, field, None)
        p.id = anon_func(p.id)
        users.append(p)
    return users
