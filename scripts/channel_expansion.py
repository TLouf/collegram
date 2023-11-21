import datetime
import itertools
import json
import logging
import os
from pathlib import Path
from pprint import pprint

import polars as pl
from dotenv import load_dotenv

import collegram

logger = logging.getLogger(__name__)


def save_all_chats_messages(client, chat, global_dt_to, paths, all_media_dict, anonymiser, interval='1mo'):
    forwarded_channels = set()
    chat_dir_path = paths.raw_data / 'messages' / f"{chat.id}"
    anon_map_save_path = paths.raw_data / 'anon_maps' / f"{chat.id}.json"
    anonymiser.update_from_disk(anon_map_save_path)

    logger.info(f"saving messages to {chat_dir_path}")
    dt_from = chat.date
    dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    dt_bin_edges = pl.datetime_range(dt_from, global_dt_to, interval=interval, eager=True, time_zone='UTC')
    for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
        messages_save_path = chat_dir_path / f"{dt_from.date()}_to_{dt_to.date()}.jsonl"
        if not messages_save_path.exists():
            messages = collegram.messages.get_channel_messages(client, chat, dt_from, dt_to, anonymiser.anonymise)
            messages_save_path.parent.mkdir(exist_ok=True, parents=True)
            messages_save_path.write_text("\n".join([m.to_json() for m in messages]))

            anon_map_save_path.write_text(json.dumps(anonymiser.anon_map))

            forwarded_channels = forwarded_channels.union(collegram.channels.from_forwarded(messages))
            all_media_dict.update(collegram.media.get_downloadable_media(messages, only_photos=True))
            break
    forwarded_channels.discard(chat.id)
    return forwarded_channels


if __name__ == '__main__':
    load_dotenv()
    paths = collegram.paths.ProjectPaths()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Go up to 30 days ago so that view counts, etc, are more or less to their final value
    global_dt_to = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    # dt_from = dt_to - datetime.timedelta(days=31)
    client = collegram.client.connect(
        os.environ['API_ID'], os.environ['API_HASH'], os.environ["PHONE_NUMBER"],
        session=str(paths.proj / 'anon.session')
    )
    all_media_dict = {}
    # channels = (paths.ext_data / "channels.txt").read_text().strip().split(",")
    channels = (paths.interim_data / "channels.txt").read_text().strip().split("\n")
    # tgdb_channels = collegram.channels.search_from_tgdb(client, "climate change")
    # api_channels = collegram.channels.search_from_api(client, "climate change")
    # channels = itertools.chain(tgdb_channels, api_channels)
    logger.info(f"{list(channels)}")
    channels = set([channels[-1]])
    processed_channels = set()
    nr_remaining_channels = len(channels)
    nr_processed_channels = 0
    while nr_remaining_channels > 0 and nr_processed_channels < 2:
        anonymiser = collegram.utils.HMAC_anonymiser()
        channel_identifier = channels.pop()
        # The `channel_identifier` here can refer to a specific chat of a channel, in
        # which case it can only be an int, or to a whole channel, in which case it can
        # be a str or an int. So first we get the encompassing full channel, to then
        # read all of its chats.
        channel_data = collegram.channels.get_full(client, channel_identifier)
        if channel_data is None:
            logger.info(f"could not get data for channel {channel_identifier}")
            continue

        channel_dict = channel_data.full_chat.to_dict()
        chans_with_uname = [c for c in channel_data.chats if getattr(c, "username", None) is not None]
        # Using hierarchy broadcast > giga > mega?
        list_unames = [c.username for c in chans_with_uname if c.broadcast]
        if len(list_unames) == 0:
            list_unames = [c.username for c in chans_with_uname if c.gigagroup]
        if len(list_unames) == 0:
            list_unames = [c.username for c in chans_with_uname if c.megagroup]
        assert len(list_unames) == 1
        channel_username = list_unames[0]
        logger.info(f'---------------- {channel_username} ----------------')
        logger.info(f"{channel_dict.get('participants_count')} participants, {channel_dict.get('about')}")

        channel_save_path = paths.raw_data / 'channels' / f"{channel_username}.json"
        channel_save_path.parent.mkdir(exist_ok=True, parents=True)
        if channel_save_path.exists():
            channel_saved_data = json.loads(channel_save_path.read_text())
            new_channels = set(channel_saved_data.get("forwards_from", {}))
        else:
            channel_saved_data = {}
            new_channels = set()

        users_list = collegram.users.get_channel_users(
            client, channel_username, anonymiser.anonymise
        ) if channel_dict.get('can_view_participants') else []

        channel_data.full_chat = collegram.channels.anonymise_full_chat(
            channel_data.full_chat, anonymiser.anonymise
        )
        channel_save_data = json.loads(channel_data.to_json())
        channel_save_data['participants'] = [json.loads(u.to_json()) for u in users_list]

        channels_chats = [
            (i, c)
            for i, c in enumerate(channel_data.chats)
            if not getattr(c, "deactivated", False)
        ]

        for i_chat, chat in channels_chats:
            # For messages, do everything chat-wise
            forwarded_channels = save_all_chats_messages(
                client, chat, global_dt_to, paths, all_media_dict, anonymiser, interval='1mo'
            )

            anon_map_save_path = paths.raw_data / 'anon_maps' / f"{chat.id}.json"
            anon_map_save_path.parent.mkdir(exist_ok=True, parents=True)
            anonymiser.update_from_disk(anon_map_save_path)
            inverse_anon_map = anonymiser.inverse_anon_map
            saved_fwd_from = set(
                [inverse_anon_map.get(c, c) for c in channel_saved_data['chats'][i_chat]['forwards_from']]
                if 'chats' in channel_saved_data
                else []
            )
            all_forwarded_channels = forwarded_channels.union(saved_fwd_from)
            new_channels = new_channels.union(all_forwarded_channels)
            processed_channels.add(chat.id)
            # Might seem redundant to write for every chat, but actually by doing this
            # single small write to disk, if the connection crashes between two chats
            # this info will still have been saved.
            channel_save_data['chats'][i_chat] = collegram.channels.get_chat_save_dict(
                chat, forwarded_channels, anonymiser.anonymise
            )
            channel_save_path.write_text(json.dumps(channel_save_data))
            nr_processed_channels += 1
            break

        # TODO: Reevaluate if save users in separate file worth it?
        # users_save_path = paths.raw_data / 'users' / f"{channel_username}.json"
        processed_channels.add(channel_username)
        channels = channels.union(new_channels).difference(processed_channels)

        nr_remaining_channels = len(channels)
        break
    # collegram.media.download_from_dict(client, all_media_dict, paths.raw_data / 'media')
