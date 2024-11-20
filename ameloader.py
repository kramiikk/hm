from .. import loader
import re
import os
import urllib.parse

@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика."""

    strings = {"name": "AmeChangeLoaderText"}

    async def updateloadercmd(self, message):
        cmd = message.raw_text.split(maxsplit=1)
        if len(cmd) == 1:
            await message.edit(self.strings("help"))
            return
        try:
            args = cmd[1].strip()
            main_file_path = os.path.join("hikka", "main.py")

            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Захватываем блок send_animation с параметрами
            animation_pattern = re.compile(r"""
                await \s+ client \. hikka_inline \. bot \. send_animation  # Начало вызова send_animation
                \(
                .*?  # Любые символы между скобками (не жадный поиск)
                caption \s* = \s* \(  # Начало аргумента caption
                \s* (
                    \"\"\"(.*?) \"\"\"  |  # Тройные кавычки
                    '(.*?)'  |  # Одинарные кавычки
                    \"(.*?)\"  # Двойные кавычки
                )
                \s* \)  # Конец аргумента caption
                .*?  # Любые символы после caption (не жадный поиск)
                \)  # Конец вызова send_animation
            """, re.VERBOSE | re.DOTALL)

            match = animation_pattern.search(content)
            if not match:
                raise ValueError("Не удалось найти блок отправки анимации в main.py")
            old_block = match.group(0)
            caption_match = match.group(2) or match.group(3) or match.group(4)  # Получаем текст подписи
            await message.reply(f"Old Block: {old_block}")

            if self._is_valid_url(args):
                new_block = re.sub(r'"([^"]+)"(?=,\s*caption)', f'"{args}"', old_block)
            else:
                new_block = old_block.replace(caption_match, args)  # Заменяем только текст подписи
            await message.reply(f"New Block: {new_block}")

            # Заменяем старый блок на новый
            updated_content = content.replace(old_block, new_block)
            await message.reply(f"Updated Content: {updated_content[:500]}...")

            try:
                with open(main_file_path, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                await message.edit(
                    f"✅ Обновлено на: <code>{args}</code>\nНапишите <code>.restart -f</code>"
                )
            except OSError as e:
                await message.edit(f"❌ Ошибка записи в файл: {e}")
        except Exception as e:
            await message.edit(f"❌ Ошибка: <code>{str(e)}</code>")

    def _is_valid_url(self, url):
        """Проверяет URL."""
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc]) and (
                url.lower().endswith(".mp4") or url.lower().endswith(".gif")
            )
        except:
            return False