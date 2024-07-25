import logging

from telethon import TelegramClient
from telethon.sessions import Session, StringSession

logger = logging.getLogger(__name__)


def connect(api_id, api_hash, phone_nr, session="anon", **client_kwargs):
    client = TelegramClient(session, api_id, api_hash, **client_kwargs)
    client.start(phone_nr)
    logger.info("Client Created")
    return client


def string_session_from(session: Session):
    s = StringSession()
    s.set_dc(session.dc_id, session.server_address, session.port)
    s.auth_key = session.auth_key
    return s
