import asyncio
from .. import loader


@loader.tds
class ealler(loader.Module):
    """post"""

    strings = {"name": "ealler"}

    THR = 1

    async def watcher(self, m):
        """channel"""
        if not m or m.chat_id != 5274754956 or "int" not in m.text:
            return
        txt = "<i>Pursue your course, let people talk!</i> "
        txt += "<emoji document_id=5289851884062915756>➕</emoji>"
        while True:
            await asyncio.sleep(1.5)
            await self.client.send_message(1868163414, "{} | {}".format(self.THR, txt))
            self.THR += 1
