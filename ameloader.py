from .. import loader
import os
import re
import urllib.parse


@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика."""

    strings = {"name": "AmeChangeLoaderText"}

    PLACEHOLDERS = {
        "version": "'.'.join(map(str, version))",
        "build": "build",
        "build_hash": "build[:7]",
        "upd": "upd",
        "web_url": "web_url",
    }

    async def updateloadercmd(self, message):
        """Обновляет текст и баннер загрузчика."""
        cmd = message.raw_text.split(maxsplit=1)

        if len(cmd) == 1:
            await message.edit(
                "<b>📋 Справка по AmeChangeLoaderText:</b>\n\n"
                "• <code>.updateloader reset hikari</code> - Сброс к настройкам Hikka\n"
                "• <code>.updateloader reset coddrago</code> - Сброс к настройкам CodDrago\n"
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

            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Находим блок кода с отправкой анимации

            animation_block_pattern = (
                r"await client\.hikka_inline\.bot\.send_animation\([^)]+\)"
            )
            animation_block = re.search(animation_block_pattern, content)

            if not animation_block:
                raise ValueError("Не удалось найти блок отправки анимации в main.py")
            # Получаем текущий URL баннера

            current_url_pattern = r'"(https://[^"]+\.mp4)"'
            current_url_match = re.search(current_url_pattern, animation_block.group(0))
            if not current_url_match:
                current_url = "https://x0.at/pYQV.mp4"  # Дефолтный URL
            else:
                current_url = current_url_match.group(1)
            if args.startswith("reset"):
                reset_args = args.split()
                if len(reset_args) < 2:
                    raise ValueError("Укажите тип сброса: hikari или coddrago")
                reset_type = reset_args[1]
                if reset_type == "hikari":
                    url = "https://github.com/hikariatama/assets/raw/master/hikka_banner.mp4"
                    repo_link = "https://github.com/hikariatama/Hikka"
                elif reset_type == "coddrago":
                    url = "https://x0.at/pYQV.mp4"
                    repo_link = "https://github.com/coddrago/Hikka"
                else:
                    raise ValueError(
                        "Неверный тип сброса. Используйте hikari или coddrago"
                    )
                new_animation_block = (
                    "await client.hikka_inline.bot.send_animation(\n"
                    "                logging.getLogger().handlers[0].get_logid_by_client(client.tg_id),\n"
                    f'                "{url}",\n'
                    '                caption=f"🌘 <b>Hikka {version} started!</b>\\n\\n'
                    f'🌳 <b>GitHub commit SHA: <a href="{repo_link}/commit/{{build_hash}}">{{build_hash}}</a></b>\\n'
                    '✊ <b>Update status: {upd}</b>\\n<b>{web_url}</b>",\n'
                    '                version=".".join(map(str, version)),\n'
                    "                build_hash=build[:7],\n"
                    "                upd=upd,\n"
                    "                web_url=web_url\n"
                    "            )"
                )
            elif self._is_valid_url(args):
                new_animation_block = animation_block.group(0).replace(
                    current_url, args
                )
            else:
                user_text = args.replace('"', '\\"')
                has_placeholders = any(
                    f"{{{k}}}" in user_text for k in self.PLACEHOLDERS.keys()
                )

                if has_placeholders:
                    used_placeholders = []
                    for name, value in self.PLACEHOLDERS.items():
                        if f"{{{name}}}" in user_text:
                            used_placeholders.append(f"{name}={value}")
                    new_animation_block = (
                        "await client.hikka_inline.bot.send_animation(\n"
                        "                logging.getLogger().handlers[0].get_logid_by_client(client.tg_id),\n"
                        f'                "{current_url}",\n'  # Используем текущий URL
                        f'                caption=f"{user_text}",\n'
                        f'                {", ".join(used_placeholders)}\n'
                        "            )"
                    )
                else:
                    new_animation_block = (
                        "await client.hikka_inline.bot.send_animation(\n"
                        "                logging.getLogger().handlers[0].get_logid_by_client(client.tg_id),\n"
                        f'                "{current_url}",\n'  # Используем текущий URL
                        f'                caption="{user_text}"\n'
                        "            )"
                    )
            # Заменяем старый блок на новый

            content = content.replace(animation_block.group(0), new_animation_block)

            with open(main_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            result_message = ""
            if args.startswith("reset"):
                result_message = f"✅ Восстановлены дефолтные настройки {reset_type}"
            elif self._is_valid_url(args):
                result_message = f"✅ Баннер обновлен на: <code>{args}</code>"
            else:
                result_message = f"✅ Текст обновлен на: <code>{user_text}</code>"
            await message.edit(f"{result_message}\nНапишите <code>.restart -f</code>")
        except Exception as e:
            await message.edit(f"❌ Ошибка: <code>{str(e)}</code>")

    def _is_valid_url(self, url):
        """Проверяет, является ли URL валидным и оканчивается на .mp4."""
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc]) and url.endswith(".mp4")
        except:
            return False
