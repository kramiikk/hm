from .. import loader
import os
import re
import urllib.parse


@loader.tds
class AmeChangeLoaderText(loader.Module):
    """Модуль для изменения текста и баннера загрузчика."""

    strings = {"name": "AmeChangeLoaderText"}

    async def updateloadercmd(self, message):
        """Обновляет текст и баннер загрузчика."""
        args = message.raw_text.split(" ", 3)

        if len(args) == 1:
            await message.edit(
                "<b>📋 Справка по AmeChangeLoaderText:</b>\n\n"
                "• <code>.updateloader reset hikari</code> - Сброс к настройкам Hikka\n"
                "• <code>.updateloader reset coddrago</code> - Сброс к настройкам CodDrago\n"
                "• <code>.updateloader https://site.com/banner.mp4</code> - Заменить баннер\n"
                "• <code>.updateloader текст</code> - Заменить текст\n\n"
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
                caption = (
                    'caption=(\n                    "🌘 <b>Hikka {} started!</b>\n\n🌳 <b>GitHub commit SHA: <a"'
                    f' href="{repo_link}/commit/{{}}">{{}}</a></b>\n✊'
                    ' <b>Update status: {}</b>\n<b>{}</b>".format(\n'
                    '                        ".".join(list(map(str, list(version)))),\n'
                    "                        build,\n"
                    "                        build[:7],\n"
                    "                        upd,\n"
                    "                        web_url,\n"
                    "                    )\n                ),"
                )

                content = self._replace_banner_url(content, url)
                content = re.sub(r"caption=\([\s\S]*?\),", caption, content)
                result_message = f"✅ Восстановлены дефолтные настройки {reset_type}"
            elif self._is_valid_url(args[1]):
                content = self._replace_banner_url(content, args[1])
                result_message = f"✅ Баннер обновлен на: <code>{args[1]}</code>"
            else:
                custom_text = re.escape(args[1])
                content = re.sub(
                    r"caption=\([\s\S]*?\),",
                    f'caption=(\n                    "{custom_text}"\n                ),',
                    content,
                )
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

    def _replace_banner_url(self, content, new_url):
        """Заменяет URL баннера в контенте файла."""
        return re.sub(
            r"([\'\"]https://)[^\'\"]+(\.mp4[\'\"])",
            f"\\1{new_url}\\2",
            content,
        )
