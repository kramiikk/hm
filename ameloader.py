from .. import loader
import os
import re
import urllib.parse


@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика."""

    strings = {"name": "AmeChangeLoaderText"}

    # Добавим словарь для определения переменных
    PLACEHOLDERS = {
        "version": "'.'.join(map(str, version))",
        "build": "build",
        "build_hash": "build[:7]",
        "upd": "upd",
        "web_url": "web_url"
    }

    async def updateloadercmd(self, message):
        """Обновляет текст и баннер загрузчика."""
        args = message.raw_text.split(" ", 3)

        if len(args) == 1:
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
            main_file_path = os.path.join("hikka", "main.py")
            with open(main_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            if args[1] == "reset":
                reset_type = args[2]
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

                animation_pattern = r'(await client\.hikka_inline\.bot\.send_animation\([^,]+,\s*)(["\']https://[^"\']+\.mp4["\'])'
                content = re.sub(animation_pattern, rf'\1"{url}"', content)

                # Формируем строки определений для каждого плейсхолдера
                placeholder_defs = []
                for name, value in self.PLACEHOLDERS.items():
                    placeholder_defs.append(f"{name}={value}")
                
                caption = (
                    'caption=(\n                    f"🌘 <b>Hikka {version} started!</b>\\n\\n'
                    '🌳 <b>GitHub commit SHA: <a href=\\"{}/commit/{build_hash}\\">{build_hash}</a></b>\\n'
                    '✊ <b>Update status: {upd}</b>\\n<b>{web_url}</b>",\n'
                    f'                    {",".join(placeholder_defs)}\n'
                    "                ),"
                ).format(repo_link)

                content = re.sub(r"caption=\([\s\S]*?\),", caption, content)
                result_message = f"✅ Восстановлены дефолтные настройки {reset_type}"
            elif self._is_valid_url(args[1]):
                animation_pattern = r'(await client\.hikka_inline\.bot\.send_animation\([^,]+,\s*)(["\']https://[^"\']+\.mp4["\'])'
                content = re.sub(animation_pattern, rf'\1"{args[1]}"', content)
                result_message = f"✅ Баннер обновлен на: <code>{args[1]}</code>"
            else:
                user_text = re.escape(args[1])
                has_placeholders = any(x in args[1] for x in [f"{{{k}}}" for k in self.PLACEHOLDERS.keys()])
                
                if has_placeholders:
                    # Формируем строки определений только для используемых плейсхолдеров
                    used_placeholders = []
                    for name in self.PLACEHOLDERS.keys():
                        if f"{{{name}}}" in args[1]:
                            used_placeholders.append(f"{name}={self.PLACEHOLDERS[name]}")
                    
                    custom_text = (
                        'caption=(\n                    f"'
                        + user_text
                        + '",\n'
                        + "                    " + ",\n                    ".join(used_placeholders) + "\n"
                        + "                ),"
                    )
                else:
                    custom_text = (
                        'caption=(\n                    f"'
                        + user_text
                        + '"\n                ),'
                    )
                
                content = re.sub(r"caption=\([\s\S]*?\),", custom_text, content)
                result_message = f"✅ Текст обновлен на: <code>{args[1]}</code>"

            with open(main_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            await message.edit(f"{result_message}\nНапишите <code>.restart -f</code>")
        except Exception as e:
            await message.edit(f"❌ Ошибка: <code>{e}</code>")

    def _is_valid_url(self, url):
        """Проверяет, является ли URL валидным и оканчивается на .mp4."""
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc]) and url.endswith(".mp4")
        except:
            return False
