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
        self.db = db

    async def watcher(self, m):
        """алко"""
        if not isinstance(m, Message):
            return
        if m.text.lower() == "топ":
            ass = self.db.get("Su", "as", {})
            top = "Топ багоюзеров:\n"
            for i in sorted(ass.items(), key=lambda x: x[1], reverse=True):
                top += f"\n{i[1][1]} {i[1][0]}"
            return await m.respond(top)
        if (
            not m.text.casefold().startswith("закидать ")
            or ("модер" not in m.text.lower() and "админ" not in m.text.lower())
            or m.text.count(" ") == 1
        ):
            return
        ass = self.db.get("Su", "as", {})
        send = str(m.sender_id)
        if send not in ass:
            ass.setdefault(send, [0, m.sender.first_name])
        num = random.randint(2, 5)
        ass[send][0] += num
        self.db.set("Su", "as", ass)
        for i in ("дерь", "говн", "письк", "ху", "член"):
            if i not in m.text:
                cmn = "👼🏾"
                continue
            if i in ("дерьмом", "говнищем", "говнишками"):
                cmn = "💩"
            elif i in ("письками", "хуями", "членами"):
                cmn = ". Смачно отсосали!💦💦💦🥵🥵🥵"
        await m.respond(
            f"Спасибо! Вы покормили модерку{cmn} \n{num} админа жабабота вам благодарны🌚 \n\n <b>Ваша репутация в тп: -{ass[send][0]}🤡</b>"
        )
