"""
════════════════════════1══════
    Profile Photo Repeater                       
    Developer: @xdesai                           
    Optimized: @kramiikk                         
════════════════════════2══════

Этот модуль позволяет автоматически обновлять фотографию профиля
каждые 15 минут.

📝 Команды:
    • .pfp - Запустить автообновление фото профиля
    • .pfpstop - Остановить автообновление

💡 Совет: Ответьте на фото командой .pfp или отправьте фото 
    с командой .pfp для установки его как фото профиля

⚠️ Отказ от ответственности:
    Разработчик не несет ответственности за любые проблемы,
    которые могут возникнуть с вашим аккаунтом.
"""

import asyncio
from telethon import functions, types
from .. import loader, utils


@loader.tds
class PfpRepeaterMod(loader.Module):
    """Profile Photo Repeater Module"""

    strings = {"name": "PfpRepeater"}

    def __init__(self):
        self.config = loader.ModuleConfig(
            "DELAY", 900, "Delay between profile photo updates in seconds"
        )
        self.running = False
        self.task = None
        self.photo_id = None

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        saved_id = self.db.get(self.strings["name"], "photo_id")
        if saved_id:
            self.photo_id = saved_id
            was_running = self.db.get(self.strings["name"], "running", False)
            if was_running:
                self.running = True
                self.task = asyncio.create_task(self.set_profile_photo())

    async def set_profile_photo(self):
        while self.running:
            try:
                if self.photo_id:
                    photos = await self.client(
                        functions.photos.GetUserPhotosRequest(
                            user_id="me", offset=0, max_id=0, limit=1
                        )
                    )

                    photo = None
                    for p in photos.photos:
                        if p.id == self.photo_id:
                            photo = p
                            break
                    if not photo:
                        raise Exception("Фото не найдено")
                    input_photo = types.InputPhoto(
                        id=photo.id,
                        access_hash=photo.access_hash,
                        file_reference=photo.file_reference,
                    )

                    await self.client(
                        functions.photos.UpdateProfilePhotoRequest(id=input_photo)
                    )
                await asyncio.sleep(self.config["DELAY"])
            except Exception as e:
                self.running = False
                self.db.set(self.strings["name"], "running", False)
                await self.client.send_message(
                    self.db.get(self.strings["name"], "chat_id"),
                    f"❌ Ошибка при обновлении фото: {str(e)}. Остановка модуля.",
                )
                break

    @loader.command()
    async def pfp(self, message):
        """Запустить автообновление фото профиля. Отправьте команду с фото или ответом на фото."""
        reply = await message.get_reply_message()

        target_message = (
            reply if reply and reply.photo else message if message.photo else None
        )

        if not target_message or not target_message.photo:
            await message.edit(
                "❌ Пожалуйста, отправьте команду с фото или ответом на сообщение с фото."
            )
            return
        if not self.running:
            self.photo_id = target_message.photo.id
            self.running = True

            self.db.set(self.strings["name"], "photo_id", self.photo_id)
            self.db.set(self.strings["name"], "chat_id", message.chat_id)
            self.db.set(self.strings["name"], "running", True)

            self.task = asyncio.create_task(self.set_profile_photo())
            await message.edit(
                f"✅ Запущено автообновление фото профиля каждые {self.config['DELAY']} секунд."
            )
        else:
            await message.edit("⚠️ Автообновление фото уже запущено.")

    @loader.command()
    async def pfpstop(self, message):
        """Остановить автообновление фото профиля"""
        if self.running:
            self.running = False
            if self.task:
                self.task.cancel()
            self.db.set(self.strings["name"], "running", False)
            await message.edit("🛑 Автообновление фото остановлено.")
        else:
            await message.edit("⚠️ Автообновление фото не запущено.")
