from .. import loader
import os
import re
import urllib.parse


@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика. 05"""

    strings = {"name": "AmeChangeLoaderText"}

    PLACEHOLDERS = {
        "version": "'.'.join(map(str, version))",
        "build": "build",
        "build_hash": "build[:7]",
        "upd": "upd",
        "web_url": "web_url",
    }

    def _replace_placeholders(self, text):
        for key, value in self.PLACEHOLDERS.items():
            text = text.replace(f"{{{key}}}", value)
        return text

    async def updateloadercmd(self, message):
        """
        Команда для обновления текста или баннера загрузчика.
        """
        cmd = message.raw_text.split(maxsplit=1)
        if len(cmd) == 1:
            await message.edit(
                "<b>📋 Справка по AmeChangeLoaderText:</b>\n\n"
                "• <code>.updateloader https://site.com/banner.mp4</code> - Заменить баннер\n"
                "• <code>.updateloader текст</code> - Заменить текст\n"
                "• <code>.updateloader текст с placeholder</code> - Заменить текст с переменными:\n"
                "   {version} - версия\n"
                "   {build} - полный билд\n"
                "   {build_hash} - короткий хеш билда\n"
                "   {upd} - статус обновления\n"
                "   {web_url} - веб-URL\n\n"
                "Пример:\n"
                "<code>.updateloader Статус {upd} Веб {web_url}</code>\n\n"
            )
            return
        try:
            args = cmd[1].strip()
            main_file_path = os.path.join("hikka", "main.py")

            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Более точный паттерн для поиска блока

            animation_block_pattern = (
                r"await\s+client\.hikka_inline\.bot\.send_animation\(\n"
                r'.*?caption=\(\s*"[^"]*"\s*\),\n'
                r".*?\)"
            )

            # Находим все совпадения

            animation_blocks = re.findall(animation_block_pattern, content, re.DOTALL)

            if not animation_blocks:
                raise ValueError("Не удалось найти блок отправки анимации в main.py")
            if self._is_valid_url(args):
                # Замена URL баннера

                new_block = re.sub(
                    r'"https://[^"]+\.mp4"', f'"{args}"', animation_blocks[0]
                )
            else:
                # Замена текста

                user_text = self._replace_placeholders(args)
                # Более надежная замена caption

                new_block = re.sub(
                    r'caption=\(\s*"[^"]*"\s*\)',
                    f'caption=("{user_text}")',
                    animation_blocks[0],
                )
            # Заменяем весь блок

            content = content.replace(animation_blocks[0], new_block)

            with open(main_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            await message.edit(
                f"✅ Обновлено на: <code>{args}</code>\nНапишите <code>.restart -f</code>"
            )
        except Exception as e:
            await message.edit(f"❌ Ошибка: <code>{str(e)}</code>")

    def _is_valid_url(self, url):
        """
        Проверяет, является ли URL валидным и оканчивается на .mp4.
        """
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc]) and url.lower().endswith(".mp4")
        except:
            return False
