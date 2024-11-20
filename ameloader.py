from .. import loader, utils
import os
import re
import urllib.parse


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
        cmd = utils.get_args_raw(message)  # Используем raw аргументы
        if not cmd:
            await message.edit(self.strings("help"))
            return
        try:
            args = f"{cmd}"  # Используем аргументы как есть, без обработки
            main_file_path = os.path.join("hikka", "main.py")

            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()
            pattern = r'(await\s+client\.hikka_inline\.bot\.send_animation\(\s*logging\.getLogger\(\)\.handlers\[0\]\.get_logid_by_client\(client\.tg_id\),\s*)"([^"]+)",(.*?caption=\()(.*?)(\),\s*\))\s*(\s*)\n(\s*)logging\.debug\('

            def replace_handler(match):
                prefix = match.group(1)
                current_url = match.group(2)
                caption_start = match.group(3)
                current_caption_content = match.group(4)
                caption_end = match.group(5)
                prev_line_indent = match.group(6)
                logging_indent = match.group(7)

                if self._is_valid_url(args):
                    return (
                        f'{prefix}"{args}",{caption_start}{current_caption_content}{caption_end}\n'
                        f"{prev_line_indent}{logging_indent}logging.debug("
                    )

                # Передаем строку как есть, без обработки
                new_caption_content = args

                return (
                    f'{prefix}"{current_url}",{caption_start}{new_caption_content}{caption_end}\n'
                    f"{prev_line_indent}{logging_indent}logging.debug("
                )

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
            clean_url = url.strip()
            if clean_url.startswith('"') or clean_url.startswith("'") or \
               clean_url.endswith('"') or clean_url.endswith("'"):
                return False
            
            result = urllib.parse.urlparse(clean_url)
            return all([result.scheme, result.netloc]) and (
                clean_url.lower().endswith(".mp4") or clean_url.lower().endswith(".gif")
            )
        except:
            return False
