import asyncio
import random

from telethon.tl.types import Message

from .. import loader


@loader.tds
class AssMod(loader.Module):
    """Модуль"""

    strings = {"name": "Ass"}

    async def client_ready(self, client, db):
        """ready"""
        self.client = client

    async def watcher(self, m):
        """алко"""
        if not isinstance(m, Message) and not m.text.casefold().startswith("закидать ") and ("одер" not in m.text or "мин" not in m.text) and not m.text.split(" ", 3)[2]:
            return
        cmn = m.text.split(" ", 2)[1]
        if cmn in ("говном", "дерьмом"):
            cmn = "💩"
        elif cmn in ("хуем", "членом", "хуями"):
            cmn = ". Смачно отсосали!💦💦💦🥵🥵🥵"
        else:
            cmn = "🪳"
        await m.respond(f"Спасибо! Вы покормили модерку{cmn} \n{random.randint(2, 5)} админа жабабота вам благодарны🌚")