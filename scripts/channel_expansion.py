import datetime
import itertools
import json
import logging
import logging.config
import os
from pathlib import Path
from pprint import pprint

import polars as pl
from dotenv import load_dotenv
from lingua import LanguageDetectorBuilder

import collegram

if __name__ == '__main__':
    load_dotenv()
    paths = collegram.paths.ProjectPaths()
    script_path = paths.proj / 'scripts' / __file__
    logging_conf = json.loads((script_path.parent / 'logging.json').read_text())
    logging_conf['handlers']['file_handler']['filename'] = str(script_path.with_suffix('.log'))
    logging.config.dictConfig(logging_conf)
    logger = logging.getLogger()

    # should always be strictly positive integers, since we want to avoid rabbit holes
    # very far away from initial seed and therefore increment based on parent priority
    # for their children.
    private_chans_priority = int(1e7)
    lang_priorities = {lc: 1 for lc in ['EN', 'FR', 'ES', 'DE', 'EL', 'IT', 'PL', 'RO']}
    lang_priorities['EN'] = 100
    lang_detector = LanguageDetectorBuilder.from_all_languages().build()
    channels_dir = paths.raw_data / 'channels'
    # Go up to 30 days ago so that view counts, etc, have more or less reached their final value
    global_dt_to = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    # dt_from = dt_to - datetime.timedelta(days=31)
    client = collegram.client.connect(
        os.environ['API_ID'], os.environ['API_HASH'], os.environ["PHONE_NUMBER"],
        session=str(paths.proj / 'anon.session'), flood_sleep_threshold=24*3600,
    )

    channels_first_seed = json.loads((paths.interim_data / "channels_first_seed.json").read_text())
    channels_queue = collegram.utils.UniquePriorityQueue()
    for c_id, c_hash in channels_first_seed.items():
        anonymiser = collegram.utils.HMAC_anonymiser()
        _, full_chat_d = collegram.channels.get_full(
            client, channels_dir, anonymiser.anonymise, channel_id=c_id, access_hash=c_hash,
        )
        prio = collegram.channels.get_explo_priority(
            full_chat_d, anonymiser, 0, lang_detector, lang_priorities, private_chans_priority
        )
        channels_queue.put((prio, c_id))

    processed_channels = set()
    nr_remaining_channels = channels_queue.qsize()
    nr_processed_channels = 0
    while not channels_queue.empty():
        # The `channel_identifier` here can refer to a specific chat of a channel, in
        # which case it can only be an int, or to a whole channel, in which case it can
        # be a str or an int. So first we get the encompassing full channel, to then
        # read all of its chats.
        prio, channel_identifier = channels_queue.get()
        if isinstance(channel_identifier, str) and channel_identifier.isdigit():
            channel_identifier = int(channel_identifier)
        anonymiser = collegram.utils.HMAC_anonymiser()
        listed_channel_full, listed_channel_data = collegram.channels.get_full(
            client, channels_dir, anonymiser.anonymise, channel_id=channel_identifier, force_query=True,
        )
        if listed_channel_data == {} and listed_channel_full is None:
            logger.warning(f"could not get data for listed channel {channel_identifier}")
            nr_remaining_channels -= 1
            continue

        if listed_channel_full is None:
            breakpoint()
            logger.error("NOT_HANDLED")
            continue

        new_channels = {}
        for chat in listed_channel_full.chats:
            channel_id = chat.id
            anonymiser = collegram.utils.HMAC_anonymiser()
            anon_channel_id = anonymiser.anonymise(channel_id)

            if channel_id == listed_channel_full.full_chat.id:
                channel_full = listed_channel_full
                channel_saved_data = listed_channel_data
            else:
                channel_full, channel_saved_data = collegram.channels.get_full(
                    client, channels_dir, anonymiser.anonymise, channel_id=channel_id,
                    force_query=True,
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
            logger.info(f"priority {prio}, {channel_full.full_chat.participants_count} participants, {channel_full.full_chat.about}")

            anon_map_save_path = paths.raw_data / 'anon_maps' / f"{anon_channel_id}.json"
            anonymiser.update_from_disk(anon_map_save_path)

            channel_save_path = channels_dir / f"{anon_channel_id}.json"
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

            recommended_chans = {}
            channel_save_data['recommended_channels'] = []
            for c in collegram.channels.get_recommended(client, chat):
                _, full_chat_d = collegram.channels.get_full(
                    client, channels_dir, anonymiser.anonymise, channel=c,
                )
                # Since recommended is a strong signal of homophily (at least in user
                # base), keep the parent's priority.
                recommended_chans[c.id] = prio
                channel_save_data['recommended_channels'].append(anonymiser.anonymise(c.id))

            for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items():
                count = collegram.messages.get_channel_messages_count(client, chat, f)
                channel_save_data[f"{content_type}_count"] = count

            channel_save_data['forwards_from'] = channel_saved_data.get('forwards_from', [])
            anonymiser.save_map(anon_map_save_path)
            channel_save_data['last_queried_at'] = datetime.datetime.now(datetime.UTC).isoformat()
            channel_save_path.write_text(json.dumps(channel_save_data))

            # Save messages, don't get to avoid overflowing memory.
            chat_dir_path = paths.raw_data / 'messages' / f"{anon_channel_id}"
            chat_dir_path.mkdir(exist_ok=True, parents=True)
            media_save_path = paths.raw_data / 'media'

            logger.info(f"reading/saving messages from/to {chat_dir_path}")
            dt_from = chat.date
            dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            dt_bin_edges = pl.datetime_range(dt_from, global_dt_to, interval='1mo', eager=True, time_zone='UTC')
            fwd_chans_from_saved_msg = collegram.channels.recover_fwd_from_msgs(chat_dir_path)

            forwarded_chans = {}
            existing_files = list(chat_dir_path.iterdir())
            for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
                chunk_fwds = set()
                messages_save_path = chat_dir_path / f"{dt_from.date()}_to_{dt_to.date()}.jsonl"
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
                    anonymiser.save_map(anon_map_save_path)
                    new_fwds = chunk_fwds.difference(forwarded_chans.keys())
                    for i in new_fwds:
                        _, full_chat_d = collegram.channels.get_full(
                            client, channels_dir, anonymiser.anonymise, channel_id=i,
                        )
                        forwarded_chans[i] = collegram.channels.get_explo_priority(
                            full_chat_d, anonymiser, prio, lang_detector, lang_priorities, private_chans_priority
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
                client, channels_dir, chat, chans_fwd_msg_to_query, anonymiser,
                prio, lang_detector, lang_priorities, private_chans_priority,
            )

            channel_save_data['forwards_from'] = [
                anonymiser.anonymise(c, safe=True)
                for c in set(forwarded_chans.keys()).union(id_map_fwd_chans.keys())
            ]
            anonymiser.save_map(anon_map_save_path)
            channel_save_path.write_text(json.dumps(channel_save_data))

            # What new channels should we explore?
            new_channels = {**new_channels, **forwarded_chans, **unseen_fwd_chans_from_saved_msgs, **recommended_chans}
            new_channels = {k: p for k, p in new_channels.items() if p < private_chans_priority}
            processed_channels.add(channel_id)
            nr_processed_channels += 1

        for c in set(new_channels.keys()).difference(processed_channels):
            channels_queue.put((new_channels[c], str(c)))
        nr_remaining_channels = channels_queue.qsize()
        logger.info(f"{nr_processed_channels} channels already processed, {nr_remaining_channels} to go")
