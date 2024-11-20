from .. import loader
import os
import re
import urllib.parse


@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика.2"""

    strings = {"name": "AmeChangeLoaderText"}

    strings_ru = {
        "help": "<b>📋 Справка по AmeChangeLoaderText:</b>\n\n"
        "• <code>.updateloader https://site.com/banner.mp4</code> - Заменить баннер\n"
        "• <code>.updateloader текст</code> - Заменить текст\n"
    }

    async def updateloadercmd(self, message):
        """
        Команда для обновления текста или баннера загрузчика.
        """
        cmd = message.raw_text.split(maxsplit=1)
        if len(cmd) == 1:
            await message.edit(self.strings("help"))
            return
        try:
            args = cmd[1].strip()
            main_file_path = os.path.join("hikka", "main.py")

            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Точный паттерн, соответствующий структуре в файле
            animation_pattern = (
                r'await\s+client\.hikka_inline\.bot\.send_animation\s*\(\s*'
                r'logging\.getLogger\(\)\.handlers\[0\]\.get_logid_by_client\(client\.tg_id\),\s*'
                r'"[^"]+",\s*'
                r'caption=\s*\(\s*'
                r'"[^"]+"\s*'
                r'\),?\s*'
                r'\)'
            )

            match = re.search(animation_pattern, content, re.DOTALL)
            
            if not match:
                # Если не нашли, пробуем найти более простой паттерн
                simple_pattern = r'await client\.hikka_inline\.bot\.send_animation\([^)]+\)'
                match = re.search(simple_pattern, content, re.DOTALL)
                if not match:
                    raise ValueError("Не удалось найти блок отправки анимации в main.py")

            old_block = match.group(0)
            
            if self._is_valid_url(args):
                # Заменяем URL
                new_block = re.sub(
                    r'"[^"]+"(?=,\s*caption)',  # Находим URL перед caption
                    f'"{args}"',
                    old_block
                )
            else:
                # Заменяем текст
                new_block = re.sub(
                    r'(caption=\s*\(\s*")([^"]*)(")',  # Находим текст внутри caption
                    f'\\1{args}\\3',
                    old_block
                )

            # Заменяем старый блок на новый
            updated_content = content.replace(old_block, new_block)

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
