from ..inline.types import BotInlineCall

@loader.tds
class AiMod(loader.Module):
    """🇺🇦"""

    strings = {"name": "aia"}

    async def client_ready(self, client, db) -> None:
        self.db = db
        self.client = client

    async def watcher(self, m) -> None:
        if "куат" in m.message.casefold():
            await self.inline.bot.send_message(
                1785723159, m.text, parse_mode="HTML"
            )
        elif "testo" in m.message.casefold():
            await utils.answer(m, "hi")
            await self.inline.bot.send_message(
                -1001656862928,
                "что это?",
                reply_markup=self.inline._generate_markup(
                    {
                        "text": "это",
                        "url": "https://telegram.me/fuckmasonbot?startgroup=stop",
                    }
                ),
            )
        elif "lover" in m.message.casefold():
            await self.inline.bot.send_photo(
                self.tg_id,
                photo="https://i.postimg.cc/BZK4Cwgv/mona-4.jpg",
                caption="где хуй?",
                reply_markup=self.inline._generate_markup(
                    {
                        "text": "это",
                        "url": "https://t.me/toadbothelpchat/2661777",
                    }
                ),
            )
        else:
            return
