import datetime
import json
import os

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

if __name__ == "__main__":
    load_dotenv()
    key_name = "thomas"

    paths = collegram.paths.ProjectPaths()
    logger = setup.init_logging(paths.proj / "scripts" / __file__)
    fpath_fwds_to_retrieve = paths.ext_data / "fpath_fwds_to_retrieve.jsonl"

    # should always be strictly positive integers, since we want to avoid rabbit holes
    # very far away from initial seed and therefore increment based on parent priority
    # for their children.
    private_chans_priority = int(1e7)
    lang_priorities = {lc: 1 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]}
    lang_priorities["EN"] = 2
    lang_detector = LanguageDetectorBuilder.from_all_languages().build()
    # Go up to 30 days ago so that view counts, etc, have more or less reached their final value
    global_dt_to = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=30
    )
    # dt_from = dt_to - datetime.timedelta(days=31)
    pre = f"{key_name.upper()}_"
    client = collegram.client.connect(
        os.environ[f"{pre}API_ID"],
        os.environ[f"{pre}API_HASH"],
        os.environ[f"{pre}PHONE_NUMBER"],
        session=str(paths.proj / f"{key_name}.session"),
        flood_sleep_threshold=24 * 3600,
        receive_updates=False,
        entity_cache_limit=10000,
        request_retries=1000,
    )
    generic_anonymiser = collegram.utils.HMAC_anonymiser()

    channels_first_seed = json.loads(
        (paths.interim_data / "channels_first_seed.json").read_text()
    )
    channels_queue = collegram.utils.UniquePriorityQueue()
    for c_id, c_hash in channels_first_seed.items():
        anonymiser = collegram.utils.HMAC_anonymiser()
        anon_id = anonymiser.anonymise(c_id)
        chan_paths = collegram.paths.ChannelPaths(anon_id, paths)
        anonymiser.save_path = chan_paths.anon_map
        anonymiser.update_from_disk()
        try:
            _, full_chat_d = collegram.channels.get_full(
                client,
                paths,
                anonymiser,
                key_name,
                channel_id=c_id,
                access_hash=c_hash,
            )
        except (ChannelPrivateError, UsernameInvalidError, ValueError):
            # So many wrong possible inputs from Telegram DB so we just skip. Some
            # with UsernameInvalidError or ValueError may be retrieved from other key.
            # Here `ChannelInvalidError` cannot happen because first seed consists of
            # broadcast channels only.
            continue
        prio = collegram.channels.get_explo_priority(
            full_chat_d,
            anonymiser,
            0,
            lang_detector,
            lang_priorities,
            private_chans_priority,
        )
        channels_queue.put((prio, int(c_id)))

    processed_channels = set()
    nr_remaining_channels = channels_queue.qsize()
    nr_processed_channels = 0
    while not channels_queue.empty():
        # First we get the encompassing full channel, to then read all of its chats.
        prio, channel_id = channels_queue.get()
        get_prio_kwargs = {
            "parent_priority": prio,
            "lang_detector": lang_detector,
            "lang_priorities": lang_priorities,
            "private_chans_priority": private_chans_priority,
        }
        anon_channel_id = anonymiser.anonymise(channel_id)
        chan_paths = collegram.paths.ChannelPaths(anon_channel_id, paths)
        anonymiser = collegram.utils.HMAC_anonymiser(save_path=chan_paths.anon_map)
        try:
            listed_channel_full, listed_channel_full_d = collegram.channels.get_full(
                client,
                paths,
                anonymiser,
                key_name,
                channel_id=channel_id,
                force_query=True,
            )
        except (
            ChannelInvalidError,
            ChannelPrivateError,
            UsernameInvalidError,
            ValueError,
        ):
            # For all but ChannelPrivateError, can try with another key (TODO: add to
            # list of new channels?).
            logger.warning(f"could not get data for listed channel {channel_id}")
            nr_remaining_channels -= 1
            continue

        new_channels = {}
        for chat in listed_channel_full.chats:
            channel_id = chat.id
            anon_channel_id = anonymiser.anonymise(channel_id)
            chan_paths = collegram.paths.ChannelPaths(anon_channel_id, paths)
            anonymiser = collegram.utils.HMAC_anonymiser(save_path=chan_paths.anon_map)

            if channel_id == listed_channel_full.full_chat.id:
                channel_full = listed_channel_full
                saved_channel_full_d = listed_channel_full_d
            else:
                try:
                    channel_full, saved_channel_full_d = collegram.channels.get_full(
                        client,
                        paths,
                        anonymiser,
                        key_name,
                        channel=chat,
                        channel_id=channel_id,
                        force_query=True,
                    )
                except (
                    ChannelInvalidError,
                    ChannelPrivateError,
                    UsernameInvalidError,
                    ValueError,
                ):
                    # Can be discussion group here, so include `ChannelInvalidError`.
                    logger.warning(f"could not get data for channel {channel_id}")
                    continue

            # Ensure we're using a raw `chat`, and not one from `listed_channel_full`
            # that may have been anonymised at some point.
            if chat.username:
                logger.info(f"**************** {chat.username} ****************")
            logger.info(f"---------------- {channel_id} ----------------")
            logger.info(
                f"priority {prio}, {channel_full.full_chat.participants_count} participants, {channel_full.full_chat.about}"
            )

            recommended_chans = {}
            channel_full_d = json.loads(channel_full.to_json())
            channel_full_d = collegram.channels.get_extended_save_data(
                client,
                chat,
                channel_full_d,
                anonymiser,
                paths,
                key_name,
                recommended_chans,
                **get_prio_kwargs,
            )
            # yield here, or only do `get_extended_save_data`. to yield here, have to
            # get rid of `chat` (premium querier should return fullchat JSON), load
            # anonymiser, move get_anoned_full_dict out of get_extended_save_data and
            # run it here below
            channel_full_d = collegram.channels.anon_full_dict(
                channel_full_d,
                anonymiser,
            )
            for key in ["recommended_channels", "forwards_from"]:
                channel_full_d[key] = list(
                    set(channel_full_d.get(key, [])).union(
                        saved_channel_full_d.get(key, [])
                    )
                )
            collegram.channels.save(channel_full_d, paths, key_name)
            chat_d = collegram.channels.get_matching_chat_from_full(channel_full_d)
            anon_channel_id = chat_d["id"]
            # try:
            #     input_chat = collegram.channels.get_input_chan(
            #         client, channel_full_d, key_name, channel_id,
            #     )
            # except (ChannelInvalidError, ChannelPrivateError, UsernameInvalidError, ValueError):
            #     # Can be discussion group here, so include `ChannelInvalidError`.
            #     logger.warning(f"could not get data for channel {channel_id}")
            #     continue
            # TODO: if want different keys to handle same channels, uncomment above and remove below:
            input_chat = chat

            chan_paths.messages.mkdir(exist_ok=True, parents=True)
            media_save_path = paths.raw_data / "media"

            forwarded_chans = {}
            for fwd_anon_id in set(channel_full_d["forwards_from"]):
                fwd_id = anonymiser.inverse_anon_map.get(fwd_anon_id)
                if fwd_id is None:
                    logger.error(f"issue with anon map of {channel_id}")
                    continue

                try:
                    _, full_chat_d = collegram.channels.get_full(
                        client,
                        paths,
                        anonymiser,
                        key_name,
                        channel_id=fwd_id,
                    )
                except ChannelPrivateError:
                    # These channels are valid and have been seen for sure,
                    # might be private though.
                    full_chat_d = {}
                except (ChannelInvalidError, ValueError):
                    # happens if chat's full was not saved to disk, and ID not present
                    # in session file
                    full_chat_d = {}
                    logger.error(f"issue with saved forwards of {channel_id}")
                    with open(fpath_fwds_to_retrieve, "a") as f:
                        f.write(json.dumps({channel_id: fwd_id}))
                        f.write("\n")

                forwarded_chans[int(fwd_id)] = collegram.channels.get_explo_priority(
                    full_chat_d,
                    anonymiser,
                    **get_prio_kwargs,
                )

            logger.info(f"reading/saving messages from/to {chan_paths.messages}")
            dt_from = chat.date
            dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            dt_bin_edges = pl.datetime_range(
                dt_from, global_dt_to, interval="1mo", eager=True, time_zone="UTC"
            )

            # Caution: the sorting only works because of file name format!
            existing_files = sorted(list(chan_paths.messages.iterdir()))
            for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
                chunk_fwds = set()
                messages_save_path = (
                    chan_paths.messages / f"{dt_from.date()}_to_{dt_to.date()}.jsonl"
                )
                is_last_saved_period = (
                    len(existing_files) > 0 and messages_save_path == existing_files[-1]
                )
                if (
                    not (
                        messages_save_path.exists()
                        or messages_save_path.with_suffix(".jsonl.gz").exists()
                    )
                    or is_last_saved_period
                ):
                    offset_id = 0
                    if is_last_saved_period:
                        # Get the offset in case collection was unexpectedly interrupted
                        # while writing for this time range.
                        last_message_saved = collegram.utils.read_nth_to_last_line(
                            messages_save_path
                        )
                        # Check if not empty file before reading message
                        if last_message_saved:
                            offset_id = collegram.json.read_message(
                                last_message_saved
                            ).id

                    # Save messages, don't get to avoid overflowing memory.
                    client.loop.run_until_complete(
                        collegram.messages.save_channel_messages(
                            client,
                            input_chat,
                            dt_from,
                            dt_to,
                            chunk_fwds,
                            anonymiser.anonymise,
                            messages_save_path,
                            media_save_path,
                            offset_id=offset_id,
                        )
                    )
                    anonymiser.save_map()
                    new_fwds = chunk_fwds.difference(forwarded_chans.keys())
                    for i in new_fwds:
                        try:
                            _, full_chat_d = collegram.channels.get_full(
                                client,
                                paths,
                                anonymiser,
                                key_name,
                                channel_id=i,
                            )
                        except ChannelPrivateError:
                            # These channels are valid and have been seen for sure,
                            # might be private though.
                            full_chat_d = {}
                        except (ChannelInvalidError, ValueError):
                            # This should happen extremely rarely, still haven't figured
                            # out conditions under which it does.
                            full_chat_d = {}
                            logger.error(f"new forward {i} of {channel_id} invalid?")

                        forwarded_chans[i] = collegram.channels.get_explo_priority(
                            full_chat_d,
                            anonymiser,
                            **get_prio_kwargs,
                        )

                    channel_full_d["forwards_from"] = list(
                        {
                            anonymiser.anonymise(c, safe=True)
                            for c in set(forwarded_chans.keys())
                        }.union(channel_full_d["forwards_from"])
                    )
                    anonymiser.save_map()
                    collegram.channels.save(channel_full_d, paths, key_name)

            # What new channels should we explore?
            new_channels = {**new_channels, **forwarded_chans, **recommended_chans}
            new_channels = {
                k: p for k, p in new_channels.items() if p < private_chans_priority
            }
            processed_channels.add(channel_id)
            nr_processed_channels += 1

        for c in set(new_channels.keys()).difference(processed_channels):
            channels_queue.put((new_channels[c], c))
        nr_remaining_channels = channels_queue.qsize()
        logger.info(
            f"{nr_processed_channels} channels already processed, {nr_remaining_channels} to go"
        )
