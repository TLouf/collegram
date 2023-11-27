from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from telethon.errors import FileReferenceExpiredError
from telethon.tl.types import (
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)

if TYPE_CHECKING:
    from pathlib import Path

    from telethon.tl.types import Message

logger = logging.getLogger(__name__)


def get_downloadable_media(messages: list[Message], media_dict: dict, media_save_path: Path) -> dict[int, MessageMediaDocument | MessageMediaPhoto]:
    media_dict = {}
    for m in messages:
        preprocess_from_message(m, media_dict, media_save_path)
    return media_dict


def preprocess_from_message(message: Message, media_dict: dict, media_save_path: Path):
    media = message.media
    if media is not None:
        if isinstance(media, MessageMediaPhoto):
            media_dict['photos'][str(media.photo.id)] = media
        elif isinstance(media, MessageMediaWebPage) and getattr(media.webpage, "cached_page", None) is not None:
            # Empty cached_page parts to lighten messages, save it in media folder.
            page_save_path = media_save_path / "cached_pages" / f"{media.webpage.id}.json"
            page_save_path.parent.mkdir(exist_ok=True, parents=True)
            if not page_save_path.exists():
                page_save_path.write_text(media.webpage.to_json())
            media.webpage.cached_page.blocks = []
            media.webpage.cached_page.photos = []
            media.webpage.cached_page.documents = []
        elif isinstance(media, MessageMediaDocument):
            # Document type for videos and GIFs (downloaded as silent mp4 videos).
            media_dict['documents'][str(media.document.id)] = media
    return media_dict


def download_from_dict(
    client, media_to_dl: dict[int, MessageMediaDocument | MessageMediaPhoto], savedir_path, only_photos: bool = False,
):
    savedir_path.mkdir(exist_ok=True, parents=True)
    ids_to_skip = set([p.stem for p in savedir_path.iterdir()])
    media_types = ['photos'] + (['documents'] if not only_photos else [])
    for mtype in media_types:
        ids_to_dl = set(media_to_dl[mtype].keys())
        for media_id in ids_to_dl.difference(ids_to_skip):
            try:
                client.download_media(media_to_dl[mtype][media_id], savedir_path / f"{media_id}")
            except FileReferenceExpiredError:
                logger.warning(f"Reference expired for media {media_id}")


def download_from_message_id(client, channel_username: str, message_id: int, savedir_path):
    m = client.get_messages(channel_username, ids=message_id)
    media_dict = get_downloadable_media([m])
    download_from_dict(client, media_dict, savedir_path)
