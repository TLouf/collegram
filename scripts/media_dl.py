import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

import collegram

if __name__ == "__main__":
    load_dotenv()

    chan_id = 123
    msg_id = 1
    output_path = Path(".")
    paths = collegram.paths.ProjectPaths()
    channel_d = collegram.channels.load(str(chan_id), paths)
    chat_d = collegram.channels.get_matching_chat_from_full(channel_d, chan_id)
    access_hash = chat_d["access_hash"]

    client = TelegramClient(
        "session_name",
        os.environ["API_ID"],
        os.environ["API_HASH"],
    )
    client = client.start(os.environ["PHONE_NUMBER"])

    input_chan = collegram.channels.get_input_peer(
        client, channel_id=chan_id, access_hash=access_hash
    )

    media = collegram.media.download_from_message_id(
        client, input_chan, msg_id, output_path
    )
    print(media)
