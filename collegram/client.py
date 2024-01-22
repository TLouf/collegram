import logging

import telethon.sync
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

logger = logging.getLogger(__name__)


def connect(api_id, api_hash, phone_nr, session="anon", **client_kwargs):
    client = TelegramClient(session, api_id, api_hash, **client_kwargs)
    client.start()
    logger.info("Client Created")

    if not client.is_user_authorized():
        phone_nr = phone_nr
        client.send_code_request(phone_nr)
        try:
            client.sign_in(phone_nr, input("Enter the code: "))
        except SessionPasswordNeededError:
            client.sign_in(password=input("Password: "))

    return client
