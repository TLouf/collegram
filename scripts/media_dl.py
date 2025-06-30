import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

import collegram

if __name__ == "__main__":
    load_dotenv()

    channel_username = "username"
    msg_id = 3301
    output_path = Path(".")

    client = TelegramClient(
        "session_name",
        os.environ["API_ID"],
        os.environ["API_HASH"],
    )
    client = client.start(os.environ["PHONE_NUMBER"])

    input_chan = collegram.channels.get_input_peer(
        client,
        channel_username=channel_username,
    )

    media = collegram.media.download_from_message_id(
        client, input_chan, msg_id, output_path
    )
    print(media)
