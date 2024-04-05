import datetime
import json
import os
import time

import polars as pl
import setup
from dotenv import load_dotenv
from telethon.errors import (
    UsernameInvalidError,
)

import collegram

if __name__ == "__main__":
    load_dotenv()
    key_name = "riccardo"

    paths = collegram.paths.ProjectPaths()
    msgs_path = paths.raw_data / "updates_experiment"
    msgs_path.mkdir(exist_ok=True)
    logger = setup.init_logging(paths.proj / "scripts" / __file__)

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

    usernames = (
        (paths.ext_data / "channels_update_experiment.txt").read_text().splitlines()
    )
    chans_dict = {}
    for un in usernames:
        try:
            chat = client.get_entity(un)
        except UsernameInvalidError:
            logger.error(f"{un} no longer exists")
            continue
        chans_dict[chat.id] = {"chat": chat, "msg_ids": []}

    hour_delta = datetime.timedelta(hours=1)
    dt_now = datetime.datetime.now(datetime.UTC)
    dt_from = dt_now - hour_delta
    global_dt_to = dt_now + datetime.timedelta(days=14)
    dt_bin_edges = pl.datetime_range(
        dt_from, global_dt_to, interval=hour_delta, eager=True, time_zone="UTC"
    )

    for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
        client.start()
        logger.info(f"--- starting on new hour up to {dt_to} ---")

        for chan_id, chat_d in chans_dict.items():
            logger.info(f"dealing with {chan_id} now")
            chan_msgs_path = msgs_path / f"{chan_id}.jsonl"
            chat = chat_d["chat"]
            forwards_set = set()
            media_save_path = paths.raw_data / "media"
            # update old messages. both following loop and `if` will be skipped on first
            # outer loop iter
            new_old = {}
            for message in client.iter_messages(
                entity=chat,
                ids=chat_d["msg_ids"],
            ):
                if message is None:
                    continue
                preprocessed_m = collegram.messages.preprocess(
                    message,
                    forwards_set,
                    generic_anonymiser.anonymise,
                    media_save_path,
                )
                preprocessed_m.queried_at = datetime.datetime.now(datetime.UTC)
                new_old[preprocessed_m.id] = preprocessed_m

            if chan_msgs_path.exists():
                new_saved = []
                with open(chan_msgs_path, "r") as f:
                    for line in f:
                        line = line.strip("\n ")
                        if line:
                            saved = json.loads(line)
                            new = new_old.get(saved["id"])
                            if new is None:
                                continue
                            new_d = json.loads(new.to_json())
                            saved["views"].append(new_d.get("views"))
                            saved["forwards"].append(new_d.get("forwards"))
                            saved["edit_date"].append(new_d.get("edit_date"))
                            saved["reactions"].append(new_d.get("reactions"))
                            saved["replies"]["replies"].append(
                                new_d["replies"]["replies"]
                                if new_d.get("replies") is not None
                                else 0
                            )
                            saved["queried_at"].append(new.queried_at.isoformat())
                            new_saved.append(json.dumps(saved))
                with open(chan_msgs_path, "w") as f:
                    f.write("\n".join(new_saved))
                    f.write("\n")

            if len(chat_d["msg_ids"]) < 100:
                with open(chan_msgs_path, "a") as f:
                    # save new messages
                    for message in client.iter_messages(
                        entity=chat,
                        offset_date=dt_from,
                        reverse=True,
                    ):
                        if message.date <= dt_to:
                            now = datetime.datetime.now(datetime.UTC)
                            preprocessed_m = collegram.messages.preprocess(
                                message,
                                forwards_set,
                                generic_anonymiser.anonymise,
                                media_save_path,
                            )
                            m_d = json.loads(preprocessed_m.to_json())
                            m_d["views"] = [m_d.get("views")]
                            m_d["forwards"] = [m_d.get("forwards")]
                            m_d["edit_date"] = [m_d.get("edit_date")]
                            m_d["reactions"] = [m_d.get("reactions")]
                            if preprocessed_m.replies is None:
                                m_d["replies"] = {"replies": [0]}
                            else:
                                m_d["replies"]["replies"] = [
                                    preprocessed_m.replies.replies
                                ]
                            m_d["queried_at"] = [now.isoformat()]
                            f.write(json.dumps(m_d))
                            f.write("\n")
                            chat_d["msg_ids"].append(preprocessed_m.id)
                        else:
                            break

        dt_now = datetime.datetime.now(datetime.UTC)
        time_to_sleep = (dt_to + hour_delta - dt_now).total_seconds()
        logger.info(f"sleeping for {time_to_sleep} seconds...")
        if time_to_sleep < 0:
            logger.error("woop")
            break
        client.disconnect()
        while time_to_sleep > 0:
            time.sleep(1)
            dt_now = datetime.datetime.now(datetime.UTC)
            time_to_sleep = (dt_to + hour_delta - dt_now).total_seconds()
