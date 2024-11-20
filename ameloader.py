from .. import loader
import os
import re
import urllib.parse
import logging


@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика."""

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
            pattern = r'(await\s+client\.hikka_inline\.bot\.send_animation\(\s*logging\.getLogger\(\)\.handlers\[0\]\.get_logid_by_client\(client\.tg_id\),\s*)"([^"]+)"(,\s*caption=\(.*?\))'

            def replace_handler(match):
                prefix = match.group(1)
                current_url = match.group(2)
                caption_part = match.group(3)

                if self._is_valid_url(args):
                    return f'{prefix}"{args}"{caption_part}'
                caption_pattern = r"caption=\((.+?)\)"

                def replace_caption(caption_match):
                    original_content = caption_match.group(1)

                    lines = original_content.split("\n")
                    if len(lines) > 1:
                        indent = len(lines[1]) - len(lines[1].lstrip())
                        return (
                            f'caption=(\n{" " * indent}"{args}"\n{" " * (indent - 2)})'
                        )
                    else:
                        return f'caption=("{args}")'

                new_caption_part = re.sub(
                    caption_pattern, replace_caption, caption_part, flags=re.DOTALL
                )

                return f'{prefix}"{current_url}"{new_caption_part}'

            new_content = re.sub(pattern, replace_handler, content, flags=re.DOTALL)

            try:
                with open(main_file_path, "w", encoding="utf-8") as f:
                    f.write(new_content)
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
            clean_url = url.strip("\"'")
            result = urllib.parse.urlparse(clean_url)
            return all([result.scheme, result.netloc]) and (
                clean_url.lower().endswith(".mp4") or clean_url.lower().endswith(".gif")
            )
        except:
            return False
