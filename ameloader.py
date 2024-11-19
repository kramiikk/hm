from .. import loader
import os
import re
import urllib.parse
import logging

@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика."""

    strings = {"name": "AmeChangeLoaderText"}

    PLACEHOLDERS = {
        "version": "'.'.join(map(str, __version__))",
        "build": "build",
        "build_hash": "build[:7]",
        "upd": "upd",
        "web_url": "web_url",
    }

    def _get_indentation(self, block_text):
        """
        Определяет отступы для блока кода.
        """
        lines = block_text.split("\n")
        base_indent = re.match(r"^\s*", lines[0]).group(0)
        param_indent = None
        for line in lines[1:]:
            stripped = line.lstrip()
            if stripped and not stripped.startswith("#"):
                param_indent = line[: len(line) - len(stripped)]
                break
        return base_indent, param_indent

    def _create_animation_block(self, url, text, base_indent, param_indent):
        """
        Генерирует блок отправки анимации с параметрами.
        """
        lines = [
            f"{base_indent}await client.hikka_inline.bot.send_animation(",
            f"{param_indent}logging.getLogger().handlers[0].get_logid_by_client(client.tg_id),",
            f'{param_indent}"{url}"'
        ]
        if text:
            if any(f"{{{k}}}" in text for k in self.PLACEHOLDERS.keys()):
                lines.append(f'{param_indent}caption=f"{text}"')
                for name, value in self.PLACEHOLDERS.items():
                    if f"{{{name}}}" in text:
                        lines.append(f"{param_indent}{name}={value},")
            else:
                lines.append(f'{param_indent}caption="{text}"')
        lines.append(f"{base_indent})")
        return "\n".join(lines)

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
                "Примеры:\n"
                "<code>.updateloader Апдейт {upd} | Версия {version}</code>\n"
                "<code>.updateloader Билд {build_hash} Статус {upd} Веб {web_url}</code>\n\n"
            )
            return

        try:
            args = cmd[1].strip()
            main_file_path = os.path.join("hikka", "main.py")

            # Чтение файла main.py
            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Поиск блока анимации
            animation_block_pattern = (
                r"([ \t]*)await\s+client\.hikka_inline\.bot\.send_animation\n"
                r"(?:[ \t]*[^\n]*\n)*?"
                r"[ \t]*"
            )
            animation_block_match = re.search(animation_block_pattern, content)
            if not animation_block_match:
                raise ValueError("Не удалось найти блок отправки анимации в main.py")

            full_block = animation_block_match.group(0)
            base_indent, param_indent = self._get_indentation(full_block)
            current_url = self._get_current_url(full_block)

            # Замена баннера или текста
            if self._is_valid_url(args):
                new_animation_block = self._create_animation_block(args, "", base_indent, param_indent)
                result_message = f"✅ Баннер обновлен на: <code>{args}</code>"
            else:
                user_text = args.replace('"', '\\"')
                new_animation_block = self._create_animation_block(current_url, user_text, base_indent, param_indent)
                result_message = f"✅ Текст обновлен на: <code>{user_text}</code>"

            # Замена блока в контенте
            content = content.replace(full_block, new_animation_block)

            # Сохранение файла
            with open(main_file_path, "w", encoding="utf-8") as f:
                f.write(content)

            await message.edit(f"{result_message}\nНапишите <code>.restart -f</code>")

        except Exception as e:
            await message.edit(f"❌ Ошибка: <code>{str(e)}</code>")

    def _get_current_url(self, animation_block):
        """
        Получает текущий URL из блока анимации.
        """
        current_url_pattern = r'"(https://[^"]+\.mp4)"'
        current_url_match = re.search(current_url_pattern, animation_block)
        if not current_url_match:
            raise ValueError("Не удалось найти текущий URL баннера")
        return current_url_match.group(1)

    def _is_valid_url(self, url):
        """
        Проверяет, является ли URL валидным и оканчивается на .mp4.
        """
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc]) and url.lower().endswith(".mp4")
        except:
            return False
