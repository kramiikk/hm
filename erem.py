from telethon import types
from telethon.tl.types import (
    MessageService,
    MessageActionChatJoinedByLink,
    MessageActionChatAddUser
)
import logging
import re
import shlex
from .. import loader, utils
import asyncio

logger = logging.getLogger(__name__)

def parse_arguments(args_raw):
    """Парсит аргументы командной строки с поддержкой параметров"""
    try:
        # Разбиваем строку на аргументы, учитывая кавычки
        args = shlex.split(args_raw)
    except:
        # В случае ошибки парсинга возвращаем простое разделение
        args = args_raw.split()
    
    # Значения по умолчанию
    result = {
        "group": None,
        "first_name": "",
        "last_name": "",
        "limit": 10000
    }
    
    i = 0
    while i < len(args):
        arg = args[i]
        
        # Обработка параметра лимита
        if arg in ('-l', '--limit'):
            if i + 1 < len(args):
                try:
                    result["limit"] = min(max(int(args[i + 1]), 1), 50000)
                    i += 2
                    continue
                except ValueError:
                    pass
            i += 1
            continue
            
        # Первый аргумент всегда группа
        if result["group"] is None:
            result["group"] = arg
        # Второй аргумент - имя
        elif not result["first_name"] and arg != "''":
            result["first_name"] = arg
        # Третий аргумент - фамилия
        elif not result["last_name"] and arg != "''":
            result["last_name"] = arg
            
        i += 1
    
    return result if result["group"] else None

@loader.tds
class JoinSearchMod(loader.Module):
    """Поиск сообщений о присоединении пользователей к группе"""
    
    strings = {
        "name": "JoinSearch",
        "no_query": "❌ <b>Укажите аргументы!</b>",
        "searching": "🔍 <b>Начинаю поиск в группе {}\nИмя: {}\nФамилия: {}\nБудет проверено сообщений: {}</b>",
        "progress": "🔄 <b>Проверено {} служебных сообщений...\nНайдено: {}</b>",
        "no_results": "❌ <b>Результаты не найдены (проверено {} служебных сообщений)</b>",
        "results": "✅ <b>Поиск завершен в группе {}!\nПроверено служебных сообщений: {}\nНайдено совпадений: {}</b>\n\n{}",
        "group_not_found": "❌ <b>Группа не найдена</b>",
        "invalid_args": (
            "❌ <b>Неверные аргументы!</b>\n\n"
            "<b>Использование:</b>\n"
            "➠ <code>.joinsearch группа [имя] [фамилия] [-l количество]</code>\n\n"
            "<b>Параметры:</b>\n"
            "• <code>группа</code> - username или ID группы\n"
            "• <code>имя</code> - имя для поиска (опционально)\n"
            "• <code>фамилия</code> - фамилия для поиска (опционально)\n"
            "• <code>-l</code> или <code>--limit</code> - количество проверяемых сообщений\n\n"
            "<b>Примеры:</b>\n"
            "• <code>.joinsearch @group_name Иван</code>\n"
            "• <code>.joinsearch @group_name Иван Петров</code>\n"
            "• <code>.joinsearch @group_name \"\" Петров</code>\n"
            "• <code>.joinsearch @group_name Иван -l 5000</code>\n"
            "• <code>.joinsearch @group_name Иван Петров --limit 20000</code>"
        )
    }

    def __init__(self):
        self.name = self.strings["name"]
        self._running = False

    async def client_ready(self, client, db):
        self._client = client

    def _is_join_message(self, msg):
        """Проверяет, является ли сообщение сообщением о входе в группу"""
        if not isinstance(msg, MessageService):
            return False
            
        return isinstance(msg.action, (
            MessageActionChatJoinedByLink,  # Вход по ссылке
            MessageActionChatAddUser        # Добавление пользователя
        ))

    async def _get_user_name(self, client, user_id):
        """Получает имя и фамилию пользователя по ID"""
        try:
            user = await client.get_entity(user_id)
            return user.first_name or "", user.last_name or ""
        except:
            return "", ""

    def _check_match(self, first_name, last_name, search_first_name, search_last_name):
        """
        Проверяет совпадение имени и фамилии с поисковым запросом.
        Если поисковое имя или фамилия пустые - они не учитываются при поиске.
        """
        if not first_name and not last_name:
            return False
            
        first_name = first_name.lower() if first_name else ""
        last_name = last_name.lower() if last_name else ""
        search_first_name = search_first_name.lower() if search_first_name else ""
        search_last_name = search_last_name.lower() if search_last_name else ""
        
        # Если указано имя для поиска, оно должно совпадать
        if search_first_name and search_first_name not in first_name:
            return False
            
        # Если указана фамилия для поиска, она должна совпадать
        if search_last_name and search_last_name not in last_name:
            return False
            
        # Хотя бы один параметр поиска должен быть указан и совпадать
        return bool(search_first_name or search_last_name)

    async def joinsearchcmd(self, message):
        """Поиск сообщений о присоединении пользователей в указанной группе.
        Использование: .joinsearch <группа> [имя] [фамилия] [-l количество]
        Параметры:
        -l или --limit - количество проверяемых сообщений
        
        Примеры:
        .joinsearch @group_name Иван
        .joinsearch @group_name Иван Петров
        .joinsearch @group_name "" Петров
        .joinsearch @group_name Иван -l 5000
        .joinsearch @group_name Иван Петров --limit 20000"""
        
        if self._running:
            await utils.answer(message, "⚠️ <b>Поиск уже выполняется. Дождитесь завершения.</b>")
            return

        args = utils.get_args_raw(message)
        if not args:
            await utils.answer(message, self.strings["invalid_args"])
            return
            
        parsed_args = parse_arguments(args)
        if not parsed_args:
            await utils.answer(message, self.strings["invalid_args"])
            return
            
        # Проверяем, что указано хотя бы имя или фамилия
        if not parsed_args["first_name"] and not parsed_args["last_name"]:
            await utils.answer(message, self.strings["invalid_args"])
            return

        try:
            target_group = await message.client.get_entity(parsed_args["group"])
        except Exception:
            await utils.answer(message, self.strings["group_not_found"])
            return

        self._running = True
        status_message = await utils.answer(
            message, 
            self.strings["searching"].format(
                parsed_args["group"],
                parsed_args["first_name"] or "не указано",
                parsed_args["last_name"] or "не указано",
                parsed_args["limit"]
            )
        )
        
        try:
            results = []
            messages_checked = 0
            last_update = 0
            
            async for msg in message.client.iter_messages(
                target_group,
                limit=parsed_args["limit"],
                filter=types.InputMessagesFilterEmpty()
            ):
                if not self._is_join_message(msg):
                    continue
                    
                messages_checked += 1
                
                if messages_checked % 100 == 0 and messages_checked != last_update:
                    last_update = messages_checked
                    await status_message.edit(
                        self.strings["progress"].format(
                            messages_checked, len(results)
                        )
                    )
                    await asyncio.sleep(0.3)

                # Получаем ID пользователя из действия
                user_id = None
                if isinstance(msg.action, MessageActionChatAddUser):
                    user_id = msg.action.users[0] if msg.action.users else None
                elif isinstance(msg.action, MessageActionChatJoinedByLink):
                    user_id = msg.from_id.user_id if msg.from_id else None

                if user_id:
                    first_name, last_name = await self._get_user_name(message.client, user_id)
                    if self._check_match(first_name, last_name, 
                                      parsed_args["first_name"], parsed_args["last_name"]):
                        user_name = f"{first_name} {last_name}".strip()
                        action_text = "присоединился по ссылке" if isinstance(msg.action, MessageActionChatJoinedByLink) else "был добавлен"
                        results.append(f"• {user_name} {action_text} | ID: {user_id} | <a href='t.me/{target_group.username}/{msg.id}'>Ссылка</a>")
                
                if messages_checked % 50 == 0:
                    await asyncio.sleep(0.1)

            if not results:
                await utils.answer(status_message, self.strings["no_results"].format(messages_checked))
            else:
                result_text = self.strings["results"].format(
                    parsed_args["group"],
                    messages_checked,
                    len(results),
                    "\n".join(results[:50])
                )
                
                if len(results) > 50:
                    result_text += f"\n\n<b>⚠️ Показаны первые 50 из {len(results)} результатов</b>"
                
                await utils.answer(status_message, result_text)

        except Exception as e:
            await utils.answer(status_message, f"❌ <b>Произошла ошибка:</b>\n{str(e)}")
        finally:
            self._running = False
