import logging
import shlex
import time
import asyncio
from telethon import types
from telethon.tl.types import (
    MessageService,
    MessageActionChatJoinedByLink,
    MessageActionChatAddUser,
)

from .. import loader, utils
from typing import Optional, Dict, List, Tuple
from collections import deque

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 500000
UPDATE_INTERVAL = 30
STATUS_CHECK_INTERVAL = (
    50  # Изменено на 50 для отправки результатов каждые 50 сообщений
)
RESULTS_CHUNK_SIZE = 50
MAX_CONCURRENT_TASKS = 10


def parse_arguments(args_raw: str) -> Optional[Dict]:
    """Парсит аргументы командной строки с поддержкой параметров"""
    if not args_raw:
        return None
    try:
        args = shlex.split(args_raw)
    except ValueError:
        args = args_raw.split()
    if not args:
        return None
    result = {
        "group": args[0],
        "first_name": "",
        "last_name": "",
        "limit": DEFAULT_LIMIT,
        "exact_match": False,
        "show_all": False,
    }

    i = 1
    while i < len(args):
        arg = args[i]

        if arg in ("-l", "--limit"):
            if i + 1 < len(args):
                try:
                    result["limit"] = max(int(args[i + 1]), 1)
                    i += 2
                    continue
                except ValueError:
                    pass
            i += 1
            continue
        if not result["first_name"]:
            if arg in ['""', "''", '" "', "' '"]:
                result["show_all"] = True
            else:
                result["first_name"] = arg
                if f'"{arg}"' in args_raw or f"'{arg}'" in args_raw:
                    result["exact_match"] = True
        elif not result["last_name"]:
            if arg in ['""', "''", '" "', "' '"]:
                result["show_all"] = True
            else:
                result["last_name"] = arg
                if f'"{arg}"' in args_raw or f"'{arg}'" in args_raw:
                    result["exact_match"] = True
        i += 1
    return result


@loader.tds
class JoinSearchMod(loader.Module):
    """Поиск сообщений о присоединении пользователей к группе"""

    strings = {
        "name": "JoinSearch",
        "no_query": "❌ <b>Укажите аргументы!</b>",
        "searching": "🔍 <b>Начинаю поиск в группе {}\n\nПараметры поиска:\n• Имя: {}\n• Фамилия: {}\n• Лимит сообщений: {}\n• Точное совпадение: {}\n• Показать всех: {}\n\nТипы проверяемых сообщений:\n• Присоединение по ссылке\n• Добавление пользователем</b>",
        "progress": "🔄 <b>Статус поиска:\n• Проверено сообщений: {}\n• Найдено совпадений: {}\n• Скорость: ~{} сообщ./сек\n• Прошло времени: {} сек.</b>",
        "no_results": "❌ <b>Результаты не найдены\n• Всего проверено: {} сообщений\n• Затраченное время: {} сек.</b>",
        "results": "✅ <b>Результаты поиска (сообщения {}-{}):\n• Найдено в этом блоке: {}</b>\n\n{}",
        "final_results": "✅ <b>Поиск завершен в группе {}!\n• Всего проверено: {}\n• Всего найдено: {}\n• Затраченное время: {} сек.</b>",
        "group_not_found": "❌ <b>Группа не найдена</b>",
        "invalid_args": "❌ <b>Неверные аргументы!</b>",
        "search_already_running": "⚠️ <b>Поиск уже выполняется</b>",
    }

    def __init__(self):
        self.name = self.strings["name"]
        self._running = False
        self._user_cache = {}
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async def client_ready(self, client, db):
        self._client = client

    def _format_user_result(
        self, user_name: str, user_id: int, target_group: str, msg_id: int, date: str
    ) -> str:
        """Форматирует результат поиска пользователя"""
        return (
            f"• {user_name} | ID: {user_id} | "
            f"<a href='t.me/{target_group}/{msg_id}'>Ссылка</a> | "
            f"{date}"
        )

    async def _get_user_name(self, client, user_id: int) -> Tuple[str, str]:
        """Получает имя и фамилию пользователя по ID с кэшированием"""
        if not user_id:
            return "", ""
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        try:
            user = await client.get_entity(user_id)
            if not user:
                return "", ""
            result = (user.first_name.lower(), (user.last_name or "").lower())
            self._user_cache[user_id] = result
            return result
        except Exception:
            return "", ""

    def _check_match(
        self,
        first_name: str,
        last_name: str,
        search_first_name: str,
        search_last_name: str,
        exact_match: bool = False,
    ) -> bool:
        """Проверяет совпадение имени и фамилии с поисковым запросом"""
        if not search_first_name and not search_last_name:
            return False
        if exact_match:
            return (
                not search_first_name or first_name == search_first_name.lower()
            ) and (not search_last_name or last_name == search_last_name.lower())
        return (not search_first_name or search_first_name.lower() in first_name) and (
            not search_last_name or search_last_name.lower() in last_name
        )

    async def _process_message(
        self, msg, message, target_group, parsed_args
    ) -> Optional[str]:
        """Обработка одного сообщения"""
        if not msg or not message or not target_group or not parsed_args:
            return None
        if not isinstance(msg, MessageService) or not isinstance(
            msg.action, (MessageActionChatJoinedByLink, MessageActionChatAddUser)
        ):
            return None
        try:
            user_id = None
            if isinstance(msg.action, MessageActionChatAddUser) and msg.action.users:
                user_id = msg.action.users[0]
            elif isinstance(msg.action, MessageActionChatJoinedByLink) and msg.from_id:
                user_id = msg.from_id.user_id
            if not user_id:
                return None
            first_name, last_name = await self._get_user_name(message.client, user_id)
            user_name = f"{first_name}{' ' + last_name if last_name else ''}"
            date_str = msg.date.strftime("%d.%m.%Y %H:%M:%S")

            if parsed_args["show_all"] or self._check_match(
                first_name,
                last_name,
                parsed_args["first_name"],
                parsed_args["last_name"],
                parsed_args["exact_match"],
            ):
                return self._format_user_result(
                    user_name, user_id, target_group.username, msg.id, date_str
                )
            return None
        except Exception as e:
            logger.exception(f"Ошибка при обработке сообщения: {str(e)}")
            return None

    async def _update_status(
        self,
        status_message,
        messages_checked: int,
        total_results: int,
        start_time: float,
    ) -> None:
        """Обновление статуса поиска"""
        try:
            current_time = time.time()
            elapsed = current_time - start_time
            speed = messages_checked / elapsed if elapsed > 0 else 0

            await status_message.edit(
                self.strings["progress"].format(
                    messages_checked, total_results, round(speed, 1), round(elapsed, 1)
                )
            )
        except Exception as e:
            logger.exception(f"Ошибка при обновлении статуса: {str(e)}")

    async def _process_messages_batch(
        self, messages, message, target_group, parsed_args
    ):
        """Обработка пакета сообщений параллельно"""
        tasks = []
        async with self._semaphore:
            for msg in messages:
                task = asyncio.create_task(
                    self._process_message(msg, message, target_group, parsed_args)
                )
                tasks.append(task)
            results = await asyncio.gather(*tasks)
            return [r for r in results if r is not None]

    async def joinsearchcmd(self, message):
        """Поиск сообщений о присоединении пользователей в указанной группе
        Аргументы: <группа> [имя] [фамилия] [-l|--limit <число>]
        Примеры:
        .joinsearch @group "Иван" - точное совпадение по имени
        .joinsearch @group Иван - частичное совпадение
        .joinsearch @group "" - показать всех пользователей
        .joinsearch @group -l 1000 - ограничить поиск 1000 сообщениями

        Для остановки поиска используйте повторную команду."""

        if not message:
            return
        if self._running:
            self._running = False
            await utils.answer(message, self.strings["search_already_running"])
            return
        args = utils.get_args_raw(message)
        parsed_args = parse_arguments(args)
        if not parsed_args:
            await utils.answer(message, self.strings["invalid_args"])
            return
        try:
            target_group = await message.client.get_entity(parsed_args["group"])
            if not target_group:
                await utils.answer(message, self.strings["group_not_found"])
                return
        except Exception:
            await utils.answer(message, self.strings["group_not_found"])
            return
        self._running = True
        status_message = None

        try:
            total_results = 0
            messages_checked = 0
            last_progress_time = time.time()
            start_time = last_progress_time
            message_batch = []
            current_batch_results = []  # Результаты текущего блока из 50 сообщений

            status_message = await utils.answer(
                message,
                self.strings["searching"].format(
                    parsed_args["group"],
                    parsed_args["first_name"] or "не указано",
                    parsed_args["last_name"] or "не указано",
                    parsed_args["limit"],
                    "да" if parsed_args["exact_match"] else "нет",
                    "да" if parsed_args["show_all"] else "нет",
                ),
            )

            batch_start = 1  # Номер первого сообщения в текущем блоке

            async for msg in message.client.iter_messages(
                target_group,
                limit=parsed_args["limit"],
                filter=types.InputMessagesFilterEmpty(),
            ):
                if not self._running:
                    break
                messages_checked += 1
                message_batch.append(msg)

                # Обрабатываем сообщения по одному для мгновенной отправки результатов

                if len(message_batch) >= MAX_CONCURRENT_TASKS:
                    batch_results = await self._process_messages_batch(
                        message_batch, message, target_group, parsed_args
                    )
                    current_batch_results.extend(batch_results)
                    total_results += len(batch_results)
                    message_batch = []
                # Отправляем результаты каждые 50 просканированных сообщений

                if messages_checked % STATUS_CHECK_INTERVAL == 0:
                    # Отправляем найденные результаты в этом блоке

                    if current_batch_results:
                        try:
                            batch_end = messages_checked
                            await message.respond(
                                self.strings["results"].format(
                                    batch_start,
                                    batch_end,
                                    len(current_batch_results),
                                    "\n".join(current_batch_results),
                                )
                            )
                        except Exception as e:
                            logger.exception(
                                f"Ошибка при отправке результатов блока: {str(e)}"
                            )
                    # Сбрасываем результаты текущего блока и обновляем счетчики

                    current_batch_results = []
                    batch_start = messages_checked + 1

                    # Обновляем статус

                    current_time = time.time()
                    if current_time - last_progress_time >= UPDATE_INTERVAL:
                        last_progress_time = current_time
                        await self._update_status(
                            status_message, messages_checked, total_results, start_time
                        )
            # Обработка оставшихся сообщений

            if message_batch:
                batch_results = await self._process_messages_batch(
                    message_batch, message, target_group, parsed_args
                )
                current_batch_results.extend(batch_results)
                total_results += len(batch_results)
            # Отправляем последние результаты, если они есть

            if current_batch_results:
                try:
                    await message.respond(
                        self.strings["results"].format(
                            batch_start,
                            messages_checked,
                            len(current_batch_results),
                            "\n".join(current_batch_results),
                        )
                    )
                except Exception as e:
                    logger.exception(
                        f"Ошибка при отправке последних результатов: {str(e)}"
                    )
            total_time = round(time.time() - start_time, 1)

            if not self._running:
                await utils.answer(
                    status_message,
                    f"✅ <b>Поиск остановлен!\n• Проверено: {messages_checked}\n• Найдено: {total_results}\n• Затраченное время: {total_time} сек.</b>",
                )
            elif total_results == 0:
                await utils.answer(
                    status_message,
                    self.strings["no_results"].format(messages_checked, total_time),
                )
            else:
                await utils.answer(
                    status_message,
                    self.strings["final_results"].format(
                        parsed_args["group"],
                        messages_checked,
                        total_results,
                        total_time,
                    ),
                )
        except Exception as e:
            error_msg = f"❌ <b>Ошибка:</b>\n{str(e)}"
            if status_message:
                await utils.answer(status_message, error_msg)
            else:
                await utils.answer(message, error_msg)
        finally:
            self._running = False
            self._user_cache.clear()
