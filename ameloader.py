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

            animation_pattern = (
                r"(\s*await\s+client\.hikka_inline\.bot\.send_animation\(\n"
                r"\s*.*?,\n"
                r"\s*(?:\"|\')([^'\"]+)(?:\"|\'),\n"
                r"\s*caption=\(\n"
                r"\s*(\"\"\"(.*?)\"\"\"|'(.*?)'|\"(.*?)\")\n"
                r"\s*\)\n"
                r"\s*\)(?:\s*,\s*\)\s*)?)"
            )

            match = re.search(animation_pattern, content, re.DOTALL)
            if not match:
                raise ValueError("Не удалось найти блок отправки анимации в main.py")
            old_block = match.group(0)
            caption_match = match.group(3) or match.group(4) or match.group(5)
            await message.reply(f"Old Block: {old_block}")

            if self._is_valid_url(args):
                new_block = re.sub(r'"([^"]+)"(?=,\s*caption)', f'"{args}"', old_block)
            else:
                new_block = old_block.replace(caption_match, args)
            await message.reply(f"New Block: {new_block}")

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