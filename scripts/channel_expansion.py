import datetime
import itertools
import json
import logging
import logging.config
import os
from pathlib import Path
from pprint import pprint

import polars as pl
import setup
from dotenv import load_dotenv
from lingua import LanguageDetectorBuilder
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)

import collegram

if __name__ == '__main__':
    load_dotenv()
    key_name = 'thomas'

    paths = collegram.paths.ProjectPaths()
    logger = setup.init_logging(paths.proj / 'scripts' / __file__)

    # should always be strictly positive integers, since we want to avoid rabbit holes
    # very far away from initial seed and therefore increment based on parent priority
    # for their children.
    private_chans_priority = int(1e7)
    lang_priorities = {lc: 1 for lc in ['EN', 'FR', 'ES', 'DE', 'EL', 'IT', 'PL', 'RO']}
    lang_detector = LanguageDetectorBuilder.from_all_languages().build()
    # Go up to 30 days ago so that view counts, etc, have more or less reached their final value
    global_dt_to = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    # dt_from = dt_to - datetime.timedelta(days=31)
    pre = f'{key_name.upper()}_'
    client = collegram.client.connect(
        os.environ[f'{pre}API_ID'], os.environ[f'{pre}API_HASH'], os.environ[f"{pre}PHONE_NUMBER"],
        session=str(paths.proj / f'{key_name}.session'), flood_sleep_threshold=24*3600,
        receive_updates=False, entity_cache_limit=10000,
    )

    channels_first_seed = json.loads((paths.interim_data / "channels_first_seed.json").read_text())
    channels_queue = collegram.utils.UniquePriorityQueue()
    for c_id, c_hash in channels_first_seed.items():
        anonymiser = collegram.utils.HMAC_anonymiser()
        anon_id = anonymiser.anonymise(c_id)
        anonymiser.save_path = paths.raw_data / 'anon_maps' / f"{anon_id}.json"
        anonymiser.update_from_disk()
        try:
            _, full_chat_d = collegram.channels.get_full(
                client, paths, anonymiser, key_name,
                channel_id=c_id, access_hash=c_hash,
            )
        except (ChannelPrivateError, UsernameInvalidError, ValueError):
            # So many wrong possible inputs from Telegram DB so we just skip. Some
            # with UsernameInvalidError or ValueError may be retrieved from other key.
            # Here `ChannelInvalidError` cannot happen because first seed consists of
            # broadcast channels only.
            continue
        prio = collegram.channels.get_explo_priority(
            full_chat_d, anonymiser, 0, lang_detector, lang_priorities, private_chans_priority
        )
        channels_queue.put((prio, int(c_id)))

    processed_channels = set()
    nr_remaining_channels = channels_queue.qsize()
    nr_processed_channels = 0
    while not channels_queue.empty():
        # First we get the encompassing full channel, to then read all of its chats.
        prio, channel_identifier = channels_queue.get()
        get_prio_kwargs = {
            'parent_priority': prio,
            'lang_detector': lang_detector,
            'lang_priorities': lang_priorities,
            'private_chans_priority': private_chans_priority,
        }
        if isinstance(channel_identifier, str) and channel_identifier.isdigit():
            channel_identifier = int(channel_identifier)
        anonymiser = collegram.utils.HMAC_anonymiser()
        try:
            listed_channel_full, listed_channel_full_d = collegram.channels.get_full(
                client, paths, anonymiser, key_name,
                channel_id=channel_identifier, force_query=True,
            )
        except (ChannelInvalidError, ChannelPrivateError, UsernameInvalidError, ValueError):
            # For all but ChannelPrivateError, can try with another key (TODO: add to
            # list of new channels?).
            logger.warning(f"could not get data for listed channel {channel_identifier}")
            nr_remaining_channels -= 1
            continue

        new_channels = {}
        for chat in listed_channel_full.chats:
            channel_id = chat.id
            anon_channel_id = anonymiser.anonymise(channel_id)
            anon_map_save_path = paths.raw_data / 'anon_maps' / f"{anon_channel_id}.json"
            anonymiser = collegram.utils.HMAC_anonymiser(save_path=anon_map_save_path)

            if channel_id == listed_channel_full.full_chat.id:
                channel_full = listed_channel_full
                saved_channel_full_d = listed_channel_full_d
            else:
                try:
                    channel_full, saved_channel_full_d = collegram.channels.get_full(
                        client, paths, anonymiser, key_name, channel=chat,
                        channel_id=channel_id, force_query=True,
                    )
                except (ChannelInvalidError, ChannelPrivateError, UsernameInvalidError, ValueError):
                    # Can be discussion group here, so include `ChannelInvalidError`.
                    logger.warning(f"could not get data for channel {channel_id}")
                    continue

            # Ensure we're using a raw `chat`, and not one from `listed_channel_full`
            # that may have been anonymised at some point.
            if chat.username:
                logger.info(f'**************** {chat.username} ****************')
            logger.info(f'---------------- {channel_id} ----------------')
            logger.info(f"priority {prio}, {channel_full.full_chat.participants_count} participants, {channel_full.full_chat.about}")

            recommended_chans = {}
            channel_save_data = collegram.channels.get_extended_save_data(
                client, channel_full, channel_saved_data, anonymiser, channels_dir,
                key_name, recommended_chans, **get_prio_kwargs,
            )
            collegram.channels.save(channel_save_data, paths, key_name)
            msgs_dir_path = paths.raw_data / 'messages' / f"{anon_channel_id}"
            msgs_dir_path.mkdir(exist_ok=True, parents=True)
            media_save_path = paths.raw_data / 'media'

            logger.info(f"reading/saving messages from/to {msgs_dir_path}")
            dt_from = chat.date
            dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            dt_bin_edges = pl.datetime_range(dt_from, global_dt_to, interval='1mo', eager=True, time_zone='UTC')
            fwd_chans_from_saved_msg = collegram.channels.recover_fwd_from_msgs(msgs_dir_path)

            forwarded_chans = {}
            existing_files = list(msgs_dir_path.iterdir())
            for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
                chunk_fwds = set()
                messages_save_path = msgs_dir_path / f"{dt_from.date()}_to_{dt_to.date()}.jsonl"
                is_last_saved_period = len(existing_files) > 0 and messages_save_path == existing_files[0]
                if not messages_save_path.exists() or is_last_saved_period:
                    offset_id = 0
                    if is_last_saved_period:
                        # Get the offset in case collection was unexpectedly interrupted
                        # while writing for this time range.
                        last_message_saved = collegram.utils.read_nth_to_last_line(messages_save_path)
                        if last_message_saved:
                            # Check if not empty file before reading message
                            offset_id = collegram.json.read_message(last_message_saved).id

                    all_media_dict = {'photos': {}, 'documents': {}}
                    collegram.messages.save_channel_messages(
                        client, chat, dt_from, dt_to, chunk_fwds, anonymiser.anonymise,
                        messages_save_path, all_media_dict, media_save_path, offset_id=offset_id
                    )
                    # collegram.media.download_from_dict(client, all_media_dict, paths.raw_data / 'media', only_photos=True)
                    anonymiser.save_map()
                    new_fwds = chunk_fwds.difference(forwarded_chans.keys())
                    for i in new_fwds:
                        try:
                            _, full_chat_d = collegram.channels.get_full(
                                client, paths, anonymiser, key_name,
                                channel_id=i,
                            )
                        except ChannelPrivateError:
                            # These channels are valid and have been seen for sure,
                            # might be private though.
                            full_chat_d = {}
                        forwarded_chans[i] = collegram.channels.get_explo_priority(
                            full_chat_d, anonymiser, **get_prio_kwargs,
                        )

            # Make message queries only when strictly necessary. If the channel was seen
            # in new messages, no need to get it through `chans_fwd_msg_to_query`.
            inverse_anon_map = anonymiser.inverse_anon_map
            id_map_fwd_chans = {}
            for c in fwd_chans_from_saved_msg.keys():
                fwd_id = inverse_anon_map.get(c)
                if fwd_id is not None:
                    id_map_fwd_chans[int(fwd_id)] = c
                else:
                    logger.error(f"anon_map of {channel_id} is incomplete, {c} was not found.")

            chans_to_recover = (
                set(id_map_fwd_chans.keys())
                 .difference(forwarded_chans.keys())
            )
            chans_fwd_msg_to_query = {}
            for og_id in chans_to_recover:
                hashed_id = id_map_fwd_chans[og_id]
                chans_fwd_msg_to_query[og_id] = fwd_chans_from_saved_msg[hashed_id]

            unseen_fwd_chans_from_saved_msgs = collegram.channels.fwd_from_msg_ids(
                client, channels_dir, input_chat, chans_fwd_msg_to_query, anonymiser,
                key_name, **get_prio_kwargs,
            )

            channel_save_data['forwards_from'] = [
                anonymiser.anonymise(c, safe=True)
                for c in set(forwarded_chans.keys()).union(id_map_fwd_chans.keys())
            ]
            anonymiser.save_map()
            collegram.channels.save(channel_save_data, channels_dir, key_name)

            # What new channels should we explore?
            new_channels = {**new_channels, **forwarded_chans, **unseen_fwd_chans_from_saved_msgs, **recommended_chans}
            new_channels = {k: p for k, p in new_channels.items() if p < private_chans_priority}
            processed_channels.add(channel_id)
            nr_processed_channels += 1

        for c in set(new_channels.keys()).difference(processed_channels):
            channels_queue.put((new_channels[c], c))
        nr_remaining_channels = channels_queue.qsize()
        logger.info(f"{nr_processed_channels} channels already processed, {nr_remaining_channels} to go")
