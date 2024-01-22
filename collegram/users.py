from __future__ import annotations

import logging
import typing

import polars as pl
import telethon.sync
from telethon.errors import ChatAdminRequiredError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch, User

from collegram.utils import PY_PL_DTYPES_MAP

if typing.TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel

logger = logging.getLogger(__name__)


def get_channel_participants(client: TelegramClient, channel_username):
    offset = 0
    limit = 100
    all_participants = []

    while True:
        try:
            participants = client(
                GetParticipantsRequest(
                    channel_username,
                    ChannelParticipantsSearch(""),
                    offset,
                    limit,
                    hash=0,
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


def get_channel_users(
    client: TelegramClient, channel: str | Channel, anon_func
) -> list[User]:
    """
    We're missing the bio here, can be obtained with GetFullUserRequest
    """
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


CHANGED_USER_FIELDS = {"id": pl.Utf8}
DISCARDED_USER_FIELDS = (
    "_",
    "contact",
    "mutual_contact",
    "close_friend",
    "first_name",
    "last_name",
    "username",
    "usernames",
    "phone",
    "restriction_reason",
    "photo",
    "emoji_status",
    "color",
    "status",
)


def flatten_dict(p: dict):
    flat_p = p.copy()
    for f in DISCARDED_USER_FIELDS:
        flat_p.pop(f)
    return flat_p


def get_pl_schema():
    user_schema = {}
    annots = User.__init__.__annotations__
    for arg in set(annots.keys()).difference(DISCARDED_USER_FIELDS):
        dtype = annots[arg]
        inner_dtype = typing.get_args(dtype)
        inner_dtype = inner_dtype[0] if len(inner_dtype) > 0 else dtype
        user_schema[arg] = PY_PL_DTYPES_MAP.get(inner_dtype)
    user_schema.update(CHANGED_USER_FIELDS)
    return user_schema
