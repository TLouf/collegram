from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

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
    from telethon.tl.types import TypeInputChannel, TypeMessageMedia

logger = logging.getLogger(__name__)

MediaDictType = dict[str, dict[str, Union[MessageMediaDocument, MessageMediaPhoto]]]


def preprocess(
    media: TypeMessageMedia,
    media_save_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
) -> int | None:
    media_id = None
    if isinstance(media, MessageMediaPhoto):
        media_id = media.photo.id

    elif isinstance(media, MessageMediaWebPage):
        media_id = media.webpage.id
        if getattr(media.webpage, "cached_page", None) is not None:
            # Empty cached_page parts to lighten messages, save it in media folder.
            page_save_path = media_save_path / "cached_pages" / f"{media_id}.json"
            parent = str(page_save_path.parent)
            fs.mkdirs(parent, exist_ok=True)
            page_save_path = str(page_save_path)
            if not fs.exists(page_save_path):
                with fs.open(page_save_path, "w") as f:
                    f.write(media.webpage.to_json())
            media.webpage.cached_page.blocks = []
            media.webpage.cached_page.photos = []
            media.webpage.cached_page.documents = []

    elif isinstance(media, MessageMediaDocument):
        # Document type for videos and GIFs (downloaded as silent mp4 videos).
        media_id = media.document.id

    return media_id


def download_from_message_id(
    client: TelegramClient,
    channel: TypeInputChannel,
    message_id: int,
    savedir_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
):
    """Download a media attached to a message.

    Cannot directly download a media because of the `file_reference` mutability:
    https://docs.telethon.dev/en/stable/quick-references/faq.html?highlight=media#can-i-send-files-by-id

    Parameters
    ----------
    client : TelegramClient
    channel : TypeInputChannel
        Reference to the channel in which the message was sent.
    message_id : int
        ID of the message inside this channel.
    savedir_path : Path
        Path leading to the directory in which to save the media. The filename will be
        the media ID.
    fs : AbstractFileSystem, optional

    Returns
    -------
    Path or None
        Path to the downloaded media. None if either the media or the message was not
        found.
    """
    m = client.loop.run_until_complete(client.get_messages(channel, ids=message_id))
    if m.media is not None:
        return download(client, m.media, savedir_path, fs=fs)


def download(
    client: TelegramClient,
    media: TypeMessageMedia,
    savedir_path: Path,
    fs: AbstractFileSystem = LOCAL_FS,
):
    media_id = preprocess(media, savedir_path, fs=fs)
    try:
        with fs.open(str(savedir_path / f"{media_id}"), "wb") as f:
            return client.loop.run_until_complete(client.download_media(media, f))
    except FileReferenceExpiredError:
        logger.warning(f"Reference expired for media {media_id}")
