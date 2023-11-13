from __future__ import annotations

from telethon.tl.types import (
    MessageMediaDocument,
    MessageMediaPhoto,
)


def get_downloadable_media(messages) -> dict[int, MessageMediaDocument | MessageMediaPhoto]:
    media_dict = {}
    for m in messages:
        # Download on web page will get social preview photo, so might as well save
        # under this photo's ID.
        if m.photo is not None:
            media_dict[str(m.photo.id)] = m.media
        elif m.document is not None:
            media_dict[str(m.document.id)] = m.media
    return media_dict


def download_from_dict(
    client, media_to_dl: dict[int, MessageMediaDocument | MessageMediaPhoto], savedir_path
):
    savedir_path.mkdir(exist_ok=True, parents=True)
    ids_to_skip = set([p.stem for p in savedir_path.iterdir()])
    ids_to_dl = set(media_to_dl.keys())
    for media_id in ids_to_dl.difference(ids_to_skip):
        client.download_media(media_to_dl[media_id], savedir_path / f"{media_id}")


def download_from_message_id(client, channel_username: str, message_id: int, savedir_path):
    m = client.get_messages(channel_username, ids=message_id)
    media_dict = get_downloadable_media([m])
    download_from_dict(client, media_dict, savedir_path)
