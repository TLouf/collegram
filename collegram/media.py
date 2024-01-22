from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from telethon.errors import FileReferenceExpiredError
from telethon.tl.types import (
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)

from collegram.utils import LOCAL_FS

if TYPE_CHECKING:
    from pathlib import Path

    from fsspec import AbstractFileSystem
    from telethon import TelegramClient
    from telethon.tl.types import Message

logger = logging.getLogger(__name__)

MediaDictType = dict[str, dict[str, MessageMediaDocument | MessageMediaPhoto]]


def preprocess_from_message(
    message: Message, media_dict: MediaDictType, media_save_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
):
    media = message.media
    if media is not None:
        if isinstance(media, MessageMediaPhoto):
            media_dict['photos'][str(media.photo.id)] = media
        elif isinstance(media, MessageMediaWebPage) and getattr(media.webpage, "cached_page", None) is not None:
            # Empty cached_page parts to lighten messages, save it in media folder.
            page_save_path = media_save_path / "cached_pages" / f"{media.webpage.id}.json"
            parent = str(page_save_path.parent)
            fs.mkdirs(parent, exist_ok=True)
            page_save_path = str(page_save_path)
            if not fs.exists(page_save_path):
                with fs.open(page_save_path, 'w') as f:
                    f.write(media.webpage.to_json())
            media.webpage.cached_page.blocks = []
            media.webpage.cached_page.photos = []
            media.webpage.cached_page.documents = []
        elif isinstance(media, MessageMediaDocument):
            # Document type for videos and GIFs (downloaded as silent mp4 videos).
            media_dict['documents'][str(media.document.id)] = media
    return media_dict


def download_from_dict(
    client: TelegramClient, media_to_dl: MediaDictType, savedir_path: Path,
    only_photos: bool = False, fs: AbstractFileSystem = LOCAL_FS,
):
    fs.mkdirs(str(savedir_path), exist_ok=True)
    ids_to_skip = set([Path(p).stem for p in fs.ls(str(savedir_path))])
    media_types = ['photos'] + (['documents'] if not only_photos else [])
    for mtype in media_types:
        ids_to_dl = set(media_to_dl[mtype].keys())
        for media_id in ids_to_dl.difference(ids_to_skip):
            try:
                with fs.open(str(savedir_path / f"{media_id}"), 'wb') as f:
                    client.download_media(media_to_dl[mtype][media_id], f)
            except FileReferenceExpiredError:
                logger.warning(f"Reference expired for media {media_id}")


def download_from_message_id(
    client: TelegramClient, channel_username: str, message_id: int, savedir_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
):
    m = client.get_messages(channel_username, ids=message_id)
    media_dict: MediaDictType = {'photos': {}, 'documents': {}}
    media_dict = preprocess_from_message(m, media_dict, savedir_path)
    download_from_dict(client, media_dict, savedir_path, fs=fs)
