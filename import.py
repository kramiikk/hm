import datetime
import random

from telethon.tl.types import InputMediaDice, Message

from .. import loader


@loader.tds
class Ass_Lib(loader.Library):
    developer = "@undick"

    async def watcher(self, m):
        """алко"""
        ct = datetime.datetime.now()
        time = ct.minute + ct.second
        tis = db.get("Su", "ti", {})
        if not isinstance(m, Message) or (
            (
                not m.dice
                or (
                    m.dice
                    and (
                        str(m.sender_id) not in tis
                        or len(tis[str(m.sender_id)]) != 4
                        or (
                            len(tis[str(m.sender_id)]) == 4
                            and -1
                            < (
                                time
                                - tis[str(m.sender_id)][
                                    0 if len(tis[str(m.sender_id)]) == 1 else 1
                                ]
                            )
                            < 1
                        )
                    )
                )
            )
            and (
                not m.text.casefold().startswith("закидать ")
                or (
                    "тп" not in m.text.casefold()
                    and "поддержку" not in m.text.casefold()
                    and "модер" not in m.text.casefold()
                    and "админ" not in m.text.casefold()
                    and "серв" not in m.text.casefold()
                )
                or m.text.count(" ") == 1
            )
            and m.text.casefold() != "сменить"
            and not m.photo
            and not m.gif
            and m.text.casefold() != "инфо"
            and m.text.casefold() != "топ"
            and m.text.casefold() != "мяу"
        ):
            return
        ass = db.get("Su", "as", {})
        ass.setdefault(str(m.sender_id), [0, m.sender.first_name, "2"])
        tis.setdefault(str(m.sender_id), [time - 7])
        dice = random.choice(("🎲", "🏀", "⚽️", "🎯", "🎳"))
        if (
            len(tis[str(m.sender_id)]) == 4
            and m.dice
            and m.media.value < tis[str(m.sender_id)][3]
        ):
            tis[str(m.sender_id)][4] = (
                await m.respond(file=InputMediaDice(dice))
            ).media.value
            self.db.set("Su", "ti", tis)
            return
        if len(tis[str(m.sender_id)]) == 3:
            await m.reply("Поиграем?😏🤭🤫")
            tis[str(m.sender_id)].append(time)
            tis[str(m.sender_id)].append(
                (await m.respond(file=InputMediaDice(dice))).media.value
            )
            return
        if m.text.casefold() == "сменить":
            a = await self.client.send_message(1688531303, m)
            ass[str(m.sender_id)] = [
                ass[str(m.sender_id)][0],
                m.sender.first_name,
                str(a.id),
            ]
            txt = "Модерация успешно подрочила😊👍"
            files = None
        elif m.text.casefold() == "инфо":
            a = await self.client.get_messages(
                1688531303, ids=int(ass[str(m.sender_id)][2])
            )
            txt = f"Имя: {ass[str(m.sender_id)][1]}\nОчки: {ass[str(m.sender_id)][0]}"
            files = a.photo if a.photo else a.gif
        elif m.text.casefold() == "топ":
            txt = "Топ багоюзеров:"
            for i in enumerate(
                sorted(ass.items(), key=lambda x: x[1], reverse=True), 1
            ):
                a = "🩲" if i[0] == 1 else i[1][1][0]
                txt += f"\n{i[0]} | {i[1][1][1]} <code>{a}</code>"
                if i[0] == 10:
                    break
            files = None
        elif m.text.casefold() == "мяу":
            txt = ""
            files = "CAADBQADOgkAAmXZgVYsIyelvGbrZgI"
        else:
            cmn = "🥞🤰🏼"
            n = 0
            if len(tis[str(m.sender_id)]) == 4 and m.dice:
                n = m.media.value
                cmn = f"🛀\n+{n} получаете за победу в этой хуйне"
            elif len(tis[str(m.sender_id)]) == 4:
                tis[str(m.sender_id)] = [time - 7]
            else:
                top = {"дерь": "💩", "говн": "💩", "письк": "💩", "ху": "🥵", "член": "🥵"}
                for i in top:
                    if i in m.text.casefold():
                        cmn = "👄 Смачно отсосали!💦💦💦🥵🥵🥵" if top[i] == "🥵" else top[i]
                        break
                cmn += f"\n{num} админа жабабота вам благодарны🎉"
            num = -n if n != 0 else random.randint(2, 5)
            ass[str(m.sender_id)][0] += num
            txt = f"Спасибо! Вы накормили модерку{cmn}\n\n <b>Ваша репутация в тп: -{ass[str(m.sender_id)][0]}🤯</b>"
            files = None
        await m.respond(message=txt, file=files)
        if -1 < (time - tis[str(m.sender_id)][go]) < 7:
            tis[str(m.sender_id)].append(time)
        else:
            tis[str(m.sender_id)] = [time]
        self.db.set("Su", "ti", tis)
        self.db.set("Su", "as", ass)
