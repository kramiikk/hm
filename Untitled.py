import random

from telethon.tl.types import Message


@loader.tds
class Ealler(loader.Module):
    """post"""

    strings = {"name": "ealler"}

    async def client_ready(self, client, db):
        self.db = db
        self.rns = await db.get("rns", "rns", 0)

    async def watcher(self, m):
        """on channel"""
        CHANNEL = -1001868163414
        if (
            not isinstance(m, Message)
            or m.chat_id == CHANNEL
            or random.random() > 1 / 13
        ):
            return
        user = await self.client.get_entity(m.sender_id)
        if user.bot or random.random() < 1 / 3:
            return
        self.rns += 1
        await self.client.send_message(
            CHANNEL,
            "<i>Pursue your course, let other people talk!</i>\n"
            + f"{self.rns} | {user.first_name}",
        )
