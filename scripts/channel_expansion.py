import datetime
import itertools
import json
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from pprint import pprint

import polars as pl
from dotenv import load_dotenv

import collegram

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    load_dotenv()
    paths = collegram.paths.ProjectPaths()
    channels_dir = paths.raw_data / 'channels'
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    script_path = paths.proj / 'scripts' / __file__
    file_handler = RotatingFileHandler(
        script_path.with_suffix('.log'), backupCount=1, maxBytes=256 * 1024, encoding="utf-8"
    )
    file_handler.setFormatter(log_formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    # Go up to 30 days ago so that view counts, etc, have more or less reached their final value
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
    chans_saved_on_disk = set([int(p.stem) for p in channels_dir.iterdir()])
    nr_remaining_channels = len(channels)
    nr_processed_channels = 0
    while nr_remaining_channels > 0:
        # The `channel_identifier` here can refer to a specific chat of a channel, in
        # which case it can only be an int, or to a whole channel, in which case it can
        # be a str or an int. So first we get the encompassing full channel, to then
        # read all of its chats.
        channel_identifier = channels.pop()
        if isinstance(channel_identifier, str) and channel_identifier.isdigit():
            channel_identifier = int(channel_identifier)
        listed_channel_full, listed_channel_data = collegram.channels.get_full(
            client, channels_dir, channel_id=channel_identifier, force_query=True,
        )
        if listed_channel_data == {} and listed_channel_full is None:
            logger.warning(f"could not get data for listed channel {channel_identifier}")
            nr_remaining_channels -= 1
            continue

        if listed_channel_full is None:
            raise ValueError('wtf')
            logger.error("NOT_HANDLED")
            continue

        new_channels = set()
        for chat in listed_channel_full.chats:
            channel_id = chat.id
            anonymiser = collegram.utils.HMAC_anonymiser()
            channel_saved_data = {}

            if channel_id == listed_channel_full.full_chat.id:
                channel_full = listed_channel_full
                channel_saved_data = listed_channel_data
            else:
                channel_full, channel_saved_data = collegram.channels.get_full(
                    client, channels_dir, channel_id=channel_id,
                    anon_func_to_save=anonymiser.anonymise, force_query=True,
                )
                if channel_saved_data == {} and channel_full is None:
                    logger.warning(f"could not get data for channel {channel_id}")
                    continue

            if channel_full is None:
                # this is possible
                logger.error(f"attached chat {channel_id} can't be queried")
                continue

            # Ensure we're using a raw `chat`, and not one from `listed_channel_full`
            # that may have been anonymised at some point.
            if chat.username:
                logger.info(f'**************** {chat.username} ****************')
            logger.info(f'---------------- {channel_id} ----------------')
            logger.info(f"{channel_full.full_chat.participants_count} participants, {channel_full.full_chat.about}")

            # TODO: filter based on language of `about`??
            anon_map_save_path = paths.raw_data / 'anon_maps' / f"{channel_id}.json"
            anonymiser.update_from_disk(anon_map_save_path)

            channel_save_path = channels_dir / f"{channel_id}.json"
            channel_save_path.parent.mkdir(exist_ok=True, parents=True)

            users_list = collegram.users.get_channel_users(
                client, chat, anonymiser.anonymise
            ) if channel_full.full_chat.can_view_participants else []

            # Might seem redundant to save before and after getting messages, but this
            # way, if the connection crashes this info will still have been saved.
            channel_save_data = collegram.channels.get_full_anon_dict(
                channel_full, anonymiser.anonymise
            )
            channel_save_data['participants'] =  [json.loads(u.to_json()) for u in users_list]
            channel_save_data['forwards_from'] = channel_saved_data.get('forwards_from', [])
            anonymiser.save_map(anon_map_save_path)
            channel_save_path.write_text(json.dumps(channel_save_data))
            chans_saved_on_disk.add(channel_id)

            # Save messages, don't get to avoid overflowing memory.
            chat_dir_path = paths.raw_data / 'messages' / f"{chat.id}"
            media_save_path = paths.raw_data / 'media'

            logger.info(f"saving messages to {chat_dir_path}")
            dt_from = chat.date
            dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            dt_bin_edges = pl.datetime_range(dt_from, global_dt_to, interval='1mo', eager=True, time_zone='UTC')
            fwd_chans_from_saved_msg = collegram.channels.recover_fwd_from_msgs(chat_dir_path)

            forwarded_chans_ids = set()
            for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
                chunk_fwds = set()
                messages_save_path = chat_dir_path / f"{dt_from.date()}_to_{dt_to.date()}.jsonl"
                messages_save_path.parent.mkdir(exist_ok=True, parents=True)
                if not messages_save_path.exists():
                    collegram.messages.save_channel_messages(
                        client, chat, dt_from, dt_to, chunk_fwds, anonymiser.anonymise,
                        messages_save_path, all_media_dict, media_save_path
                    )
                    anonymiser.save_map(anon_map_save_path)
                    new_fwds = chunk_fwds.difference(forwarded_chans_ids)
                    for i in new_fwds:
                        _, full_chat_d = collegram.channels.get_full(
                            client, channels_dir, channel_id=i, anon_func_to_save=anonymiser.anonymise
                        )
                        chans_saved_on_disk.add(i)
                    forwarded_chans_ids = forwarded_chans_ids.union(new_fwds)

            # Make message queries only when strictly necessary. If the channel was seen
            # in new messages, no need to get it through `chans_fwd_msg_to_query`.
            inverse_anon_map = anonymiser.inverse_anon_map
            fwd_chans_from_saved_msg_ids = {}
            for c in fwd_chans_from_saved_msg.keys():
                fwd_id = inverse_anon_map.get(c)
                if fwd_id is not None:
                    fwd_chans_from_saved_msg_ids[int(fwd_id)] = c
                else:
                    logger.error(f"anon_map of {channel_id} is incomplete, {c} was not found.")
            chans_to_recover = (
                set(fwd_chans_from_saved_msg_ids.keys())
                 .difference(forwarded_chans_ids)
                 .difference(chans_saved_on_disk)
            )
            chans_fwd_msg_to_query = {}
            for og_id in chans_to_recover:
                hashed_id = fwd_chans_from_saved_msg_ids[og_id]
                chans_fwd_msg_to_query[og_id] = fwd_chans_from_saved_msg[hashed_id]

            forwarded_chans_dict = collegram.channels.fwd_from_msg_ids(
                client, chat, chans_fwd_msg_to_query, channels_dir, anonymiser.anonymise
            )

            channel_save_data['forwards_from'] = [
                anonymiser.anonymise(c, safe=True) for c in forwarded_chans_ids.union(fwd_chans_from_saved_msg_ids.keys())
            ]
            anonymiser.save_map(anon_map_save_path)
            channel_save_path.write_text(json.dumps(channel_save_data))

            # What new channels should we explore?
            inverse_anon_map = anonymiser.inverse_anon_map
            saved_fwd_from = set()
            for c in channel_saved_data.get('forwards_from', []):
                fwd_id = inverse_anon_map.get(c)
                if fwd_id is not None:
                    saved_fwd_from.add(int(fwd_id))
                else:
                    logger.error(f"anon_map of {channel_id} is incomplete, {c} was not found.")
            new_channels = new_channels.union(forwarded_chans_ids).union(saved_fwd_from)
            processed_channels.add(channel_id)
            nr_processed_channels += 1
            # TODO: Reevaluate if save users in separate file worth it?
            # users_save_path = paths.raw_data / 'users' / f"{channel_username}.json"

        channels = channels.union(new_channels).difference(processed_channels)
        nr_remaining_channels = len(channels)
        logger.info(f"{nr_processed_channels} channels already processed, {nr_remaining_channels} to go")
    # collegram.media.download_from_dict(client, all_media_dict, paths.raw_data / 'media', only_photos=True)
