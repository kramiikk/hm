import asyncio
import random

from telethon.tl.types import Message

from .. import loader


@loader.tds
class krmkMod(loader.Module):
    """krmk"""

    strings = {"name": "krmk"}

    async def client_ready(self, client, db):
        """ready"""
        self.db = db
        self.client = client
        self.me = await client.get_me()
        self.thr = db.get("Thr", "thr", {})
        self.rs = db.get("Thr", "rs", {})

    async def thrcmd(self, m):
        """список чатов"""
        if len(m.text) < 5:
            txt = "Главный: "
            if "main" not in self.thr:
                txt += f"не задан\n\nСписок:"
            else:
                txt += f'<code>-{self.thr["main"]}</code>\n\nСписок:'
            if "chats" not in self.thr:
                txt += "\n Пусто"
            else:
                for i in self.thr["chats"]:
                    txt += f"\n<code>{i}<code>"
            return await m.edit(txt)
        cmn = m.text.split(" ", 2)[1]
        if cmn == "main":
            if "main" not in self.thr:
                return await m.edit("Введите ид главного чата/канала")
            msg = int(m.text.split(" ", 2)[2])
            if "-" not in str(msg):
                return await m.edit("неправильный id")
            self.thr.setdefault("main", msg)
            txt = f"🤙🏾 Главный: <code>{msg}</code>"
            self.db.set("Thr", "thr", self.thr)
            return await m.edit(txt)
        msg = int(cmn)
        if "-" not in str(msg) or len(cmn) < 9:
            return await m.edit("неправильный id")
        if "chats" in self.thr and msg in self.thr["chats"]:
            self.thr["chats"].remove(msg)
            txt = f"<code>{msg}</code><b> удален</b>"
            if self.thr["chats"] == []:
                self.thr.pop("chats")
            return await m.edit(txt)
        if "chats" in self.thr:
            txt = f"<code>{msg}</code><b> добавлен</b>"
            self.thr["chats"].append(msg)
        else:
            self.thr.setdefault("chats", [msg])
            txt = f"<code>{msg}</code><b> добавлен</b>"
        self.db.set("Thr", "thr", self.thr)
        await m.edit(txt)

    async def watcher(self, m: Message):
        """алко"""
        if not hasattr(m, "text") or not isinstance(m, Message):
            return
        if (
            "chats" not in self.thr
            or m.chat_id not in self.thr["chats"]
            or m.sender_id == self.me.id
            or m.date.minute in (0, 1, 29, 30, 31, 58, 59)
            or random.randint(0, 21) != 3
        ):
            return
        await asyncio.sleep(random.randint(3, 13) + m.date.second)
        if m.chat_id not in self.rs:
            self.rs.setdefault(m.chat_id, (m.date.hour + m.date.minute) - 10)
            self.db.set("Thr", "rs", self.rs)
        if -1 < ((m.date.hour + m.date.minute) - self.rs[m.chat_id]) < 10:
            return
        self.rs[m.chat_id] = m.date.hour + m.date.minute
        self.db.set("Thr", "rs", self.rs)
        try:
            p = await self.client.get_messages(self.thr["main"], limit=100)
        except Exception:
            return
        if p.total < 2:
            return
        p = p[random.randint(0, p.total - 2)]
        if random.randint(0, 33) != 13:
            cc = [m.chat_id]
        else:
            cc = self.thr["chats"]
        for i in cc:
            await asyncio.sleep(random.randint(1, 13))
            try:
                if p.media is not None:
                    await self.client.send_file(i, p, caption=p.text)
                else:
                    await self.client.send_message(i, p.text)
            except Exception:
                pass
