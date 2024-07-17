import logging

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import Session, StringSession

logger = logging.getLogger(__name__)


def connect(api_id, api_hash, phone_nr, session="anon", **client_kwargs):
    client = TelegramClient(session, api_id, api_hash, **client_kwargs)
    client.start()
    logger.info("Client Created")

    if not client.loop.run_until_complete(client.is_user_authorized()):
        phone_nr = phone_nr
        client.loop.run_until_complete(client.send_code_request(phone_nr))
        try:
            client.loop.run_until_complete(
                client.sign_in(phone_nr, input("Enter the code: "))
            )
        except SessionPasswordNeededError:
            client.loop.run_until_complete(client.sign_in(password=input("Password: ")))

    return client


def string_session_from(session: Session):
    s = StringSession()
    s.set_dc(session.dc_id, session.server_address, session.port)
    s.auth_key = session.auth_key
    return s
