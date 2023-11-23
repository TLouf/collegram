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
    media_save_path = paths.raw_data / 'media'
    anonymiser.update_from_disk(anon_map_save_path)

    logger.info(f"saving messages to {chat_dir_path}")
    dt_from = chat.date
    dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    dt_bin_edges = pl.datetime_range(dt_from, global_dt_to, interval=interval, eager=True, time_zone='UTC')
    for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
        messages_save_path = chat_dir_path / f"{dt_from.date()}_to_{dt_to.date()}.jsonl"
        if not messages_save_path.exists():
            messages = collegram.messages.get_channel_messages(client, chat, dt_from, dt_to, anonymiser.anonymise, all_media_dict, media_save_path)
            messages_save_path.parent.mkdir(exist_ok=True, parents=True)
            messages_save_path.write_text("\n".join([m.to_json() for m in messages]))

            anonymiser.save_map(anon_map_save_path)

            forwarded_channels = forwarded_channels.union(collegram.channels.from_forwarded(messages))
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
    all_media_dict = {'photos': {}, 'documents': {}}
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
        channel_identifier = channels.pop()
        # The `channel_identifier` here can refer to a specific chat of a channel, in
        # which case it can only be an int, or to a whole channel, in which case it can
        # be a str or an int. So first we get the encompassing full channel, to then
        # read all of its chats.
        listed_channel_data = collegram.channels.get_full(client, channel_identifier)
        if listed_channel_data is None:
            logger.info(f"could not get data for listed channel {channel_identifier}")
            continue

        for chat in listed_channel_data.chats:
            anonymiser = collegram.utils.HMAC_anonymiser()
            channel_data = (
                listed_channel_data
                if chat.id == listed_channel_data.full_chat.id
                else collegram.channels.get_full(client, channel_identifier)
            )
            if channel_data is None:
                logger.info(f"could not get data for channel {chat.id}")
                continue

            channel_id = chat.id
            logger.info(f'---------------- {channel_id} ----------------')
            logger.info(f"{channel_data.full_chat.participants_count} participants, {channel_data.full_chat.about}")

            anon_map_save_path = paths.raw_data / 'anon_maps' / f"{channel_id}.json"
            anon_map_save_path.parent.mkdir(exist_ok=True, parents=True)
            anonymiser.update_from_disk(anon_map_save_path)

            channel_save_path = paths.raw_data / 'channels' / f"{channel_id}.json"
            channel_save_path.parent.mkdir(exist_ok=True, parents=True)
            if channel_save_path.exists():
                channel_saved_data = json.loads(channel_save_path.read_text())
                new_channels = set(channel_saved_data.get("forwards_from", {}))
            else:
                channel_saved_data = {}
                new_channels = set()

            users_list = collegram.users.get_channel_users(
                client, chat, anonymiser.anonymise
            ) if channel_data.full_chat.can_view_participants else []
            # Save messages, don't get to avoid overflowing memory.
            forwarded_channels = save_all_chats_messages(
                client, chat, global_dt_to, paths, all_media_dict, anonymiser, interval='1mo'
            )

            # Might seem redundant to write for every chat, but actually by doing this
            # single small write to disk, if the connection crashes between two chats
            # this info will still have been saved. Chats should be anonymised last
            # since they're used to make participants or messages' requests.
            for c in channel_data.chats:
                c = collegram.channels.anonymise_chat(
                    c, anonymiser.anonymise, safe=True
                )
            channel_data.full_chat = collegram.channels.anonymise_full_chat(
                channel_data.full_chat, anonymiser.anonymise, safe=True
            )
            channel_save_data = json.loads(channel_data.to_json())
            # CHANGED TODO check
            channel_save_data['participants'] =  [json.loads(u.to_json()) for u in users_list]
            channel_save_data['forwards_from'] = [
                anonymiser.anonymise(c) for c in forwarded_channels
            ] # CHANGED TODO check
            anonymiser.save_map(anon_map_save_path)
            channel_save_path.write_text(json.dumps(channel_save_data))

            # What new channels should we explore?
            inverse_anon_map = anonymiser.inverse_anon_map
            saved_fwd_from = set([
                inverse_anon_map.get(c, c)
                for c in channel_saved_data.get('forwards_from', [])
            ])
            new_channels = new_channels.union(forwarded_channels).union(saved_fwd_from)
            processed_channels.add(channel_id)
            nr_processed_channels += 1
            # TODO: Reevaluate if save users in separate file worth it?
            # users_save_path = paths.raw_data / 'users' / f"{channel_username}.json"
            break

        channels = channels.union(new_channels).difference(processed_channels)
        nr_remaining_channels = len(channels)
        break
    # collegram.media.download_from_dict(client, all_media_dict, paths.raw_data / 'media', only_photos=True)
