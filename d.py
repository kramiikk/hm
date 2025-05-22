import logging
import shlex
import time
import asyncio
from datetime import datetime, timedelta, timezone
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
STATUS_CHECK_INTERVAL = 50
RESULTS_CHUNK_SIZE = 50
MAX_CONCURRENT_TASKS = 10


def parse_date(date_str: str) -> Optional[datetime]:
    """Парсит дату в различных форматах и возвращает UTC datetime"""
    formats = [
        "%d.%m.%Y",  # 01.01.2023
        "%d/%m/%Y",  # 01/01/2023
        "%Y-%m-%d",  # 2023-01-01
        "%d.%m.%Y %H:%M",  # 01.01.2023 12:30
        "%d.%m.%y",  # 01.01.23
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Преобразуем в UTC

            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Попробуем парсить относительные даты

    try:
        now_utc = datetime.now(timezone.utc)
        if date_str.endswith("d") or date_str.endswith("д"):
            days = int(date_str[:-1])
            return now_utc - timedelta(days=days)
        elif date_str.endswith("w") or date_str.endswith("н"):
            weeks = int(date_str[:-1])
            return now_utc - timedelta(weeks=weeks)
        elif date_str.endswith("m") or date_str.endswith("м"):
            months = int(date_str[:-1])
            return now_utc - timedelta(days=months * 30)
        elif date_str.endswith("y") or date_str.endswith("г"):
            years = int(date_str[:-1])
            return now_utc - timedelta(days=years * 365)
    except ValueError:
        pass
    return None


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
        "from_date": None,
        "to_date": None,
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
        elif arg in ("-f", "--from", "--from-date"):
            if i + 1 < len(args):
                date = parse_date(args[i + 1])
                if date:
                    result["from_date"] = date
                    i += 2
                    continue
            i += 1
            continue
        elif arg in ("-t", "--to", "--to-date"):
            if i + 1 < len(args):
                date = parse_date(args[i + 1])
                if date:
                    result["to_date"] = date
                    i += 2
                    continue
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
        "searching": "🔍 <b>Начинаю поиск в группе {}\n\nПараметры поиска:\n• Имя: {}\n• Фамилия: {}\n• Лимит сообщений: {}\n• Точное совпадение: {}\n• Показать всех: {}\n• Дата от: {}\n• Дата до: {}\n\nТипы проверяемых сообщений:\n• Присоединение по ссылке\n• Добавление пользователем</b>",
        "progress": "🔄 <b>Статус поиска:\n• Проверено сообщений: {}\n• Найдено совпадений: {}\n• Скорость: ~{} сообщ./сек\n• Прошло времени: {} сек.\n• Текущая дата сообщения: {}</b>",
        "no_results": "❌ <b>Результаты не найдены\n• Всего проверено: {} сообщений\n• Затраченное время: {} сек.</b>",
        "results": "✅ <b>Результаты поиска (сообщения {}-{}):\n• Найдено в этом блоке: {}</b>\n\n{}",
        "final_results": "✅ <b>Поиск завершен в группе {}!\n• Всего проверено: {}\n• Всего найдено: {}\n• Затраченное время: {} сек.</b>",
        "group_not_found": "❌ <b>Группа не найдена</b>",
        "invalid_args": "❌ <b>Неверные аргументы!</b>",
        "search_already_running": "⚠️ <b>Поиск уже выполняется</b>",
        "invalid_date": "❌ <b>Неверный формат даты!</b>",
        "date_reached": "✅ <b>Достигнута конечная дата поиска</b>",
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

    def _is_message_in_date_range(
        self, msg_date: datetime, from_date: datetime, to_date: datetime
    ) -> bool:
        """Проверяет, попадает ли сообщение в заданный диапазон дат"""
        # Убеждаемся, что msg_date имеет информацию о временной зоне

        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)
        if from_date and msg_date < from_date:
            return False
        if to_date and msg_date > to_date:
            return False
        return True

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
        # Проверяем диапазон дат

        if not self._is_message_in_date_range(
            msg.date, parsed_args["from_date"], parsed_args["to_date"]
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
        current_date: str = "",
    ) -> None:
        """Обновление статуса поиска"""
        try:
            current_time = time.time()
            elapsed = current_time - start_time
            speed = messages_checked / elapsed if elapsed > 0 else 0

            await status_message.edit(
                self.strings["progress"].format(
                    messages_checked,
                    total_results,
                    round(speed, 1),
                    round(elapsed, 1),
                    current_date,
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
        Аргументы: <группа> [имя] [фамилия] [-l|--limit <число>] [-f|--from <дата>] [-t|--to <дата>]

        Форматы дат:
        • 01.01.2023, 01/01/2023, 2023-01-01
        • 01.01.2023 12:30 (с временем)
        • 30d, 30д (30 дней назад)
        • 4w, 4н (4 недели назад)
        • 6m, 6м (6 месяцев назад)
        • 1y, 1г (1 год назад)

        Примеры:
        .joinsearch @group "Иван" -f 01.01.2023 - поиск с 1 января 2023
        .joinsearch @group Иван -f 30d - поиск за последние 30 дней
        .joinsearch @group "" -f 1y -t 6m - все пользователи с года до 6 месяцев назад
        .joinsearch @group -l 1000 -f 01.01.2023 - ограничить поиск 1000 сообщениями с даты

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
        # Проверяем корректность дат

        if parsed_args["from_date"] and parsed_args["to_date"]:
            if parsed_args["from_date"] > parsed_args["to_date"]:
                await utils.answer(
                    message, "❌ <b>Дата 'от' не может быть позже даты 'до'!</b>"
                )
                return
        self._running = True
        status_message = None

        try:
            total_results = 0
            messages_checked = 0
            last_progress_time = time.time()
            start_time = last_progress_time
            message_batch = []
            current_batch_results = []
            current_message_date = ""

            # ИСПРАВЛЕНИЕ: Определяем правильную стратегию поиска

            offset_date = None
            reverse_search = False

            if parsed_args["to_date"] and parsed_args["from_date"]:
                # Диапазон дат: начинаем с to_date и идем до from_date

                offset_date = parsed_args["to_date"]
            elif parsed_args["to_date"] and not parsed_args["from_date"]:
                # Только верхняя граница: начинаем с to_date

                offset_date = parsed_args["to_date"]
            elif parsed_args["from_date"] and not parsed_args["to_date"]:
                # Только нижняя граница: начинаем немного после from_date и ищем в обратном порядке
                # Добавляем небольшой буфер (например, 1 день) к from_date

                search_start = parsed_args["from_date"] + timedelta(days=1)
                offset_date = search_start
                reverse_search = True
            status_message = await utils.answer(
                message,
                self.strings["searching"].format(
                    parsed_args["group"],
                    parsed_args["first_name"] or "не указано",
                    parsed_args["last_name"] or "не указано",
                    parsed_args["limit"],
                    "да" if parsed_args["exact_match"] else "нет",
                    "да" if parsed_args["show_all"] else "нет",
                    (
                        parsed_args["from_date"].strftime("%d.%m.%Y %H:%M")
                        if parsed_args["from_date"]
                        else "не указано"
                    ),
                    (
                        parsed_args["to_date"].strftime("%d.%m.%Y %H:%M")
                        if parsed_args["to_date"]
                        else "не указано"
                    ),
                ),
            )

            batch_start = 1
            early_stop = False

            # Используем правильную стратегию поиска в зависимости от параметров

            iter_params = {
                "limit": parsed_args["limit"],
                "filter": types.InputMessagesFilterEmpty(),
            }

            if offset_date:
                iter_params["offset_date"] = offset_date
            if reverse_search:
                # Для обратного поиска используем reverse=True

                iter_params["reverse"] = True
            async for msg in message.client.iter_messages(target_group, **iter_params):
                if not self._running:
                    break
                # ИСПРАВЛЕНИЕ: Адаптивная проверка границ в зависимости от направления поиска

                msg_date_utc = msg.date
                if msg_date_utc.tzinfo is None:
                    msg_date_utc = msg_date_utc.replace(tzinfo=timezone.utc)
                if reverse_search:
                    # При обратном поиске (от старых к новым) останавливаемся при достижении to_date

                    if parsed_args["to_date"] and msg_date_utc > parsed_args["to_date"]:
                        early_stop = True
                        break
                else:
                    # При обычном поиске (от новых к старым) останавливаемся при достижении from_date

                    if (
                        parsed_args["from_date"]
                        and msg_date_utc < parsed_args["from_date"]
                    ):
                        early_stop = True
                        break
                messages_checked += 1
                current_message_date = msg.date.strftime("%d.%m.%Y %H:%M")
                message_batch.append(msg)

                if len(message_batch) >= MAX_CONCURRENT_TASKS:
                    batch_results = await self._process_messages_batch(
                        message_batch, message, target_group, parsed_args
                    )
                    current_batch_results.extend(batch_results)
                    total_results += len(batch_results)
                    message_batch = []
                if messages_checked % STATUS_CHECK_INTERVAL == 0:
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
                    current_batch_results = []
                    batch_start = messages_checked + 1

                    current_time = time.time()
                    if current_time - last_progress_time >= UPDATE_INTERVAL:
                        last_progress_time = current_time
                        await self._update_status(
                            status_message,
                            messages_checked,
                            total_results,
                            start_time,
                            current_message_date,
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
            elif early_stop:
                await utils.answer(
                    status_message,
                    f"✅ <b>Поиск завершен (достигнута начальная дата)!\n• Проверено: {messages_checked}\n• Найдено: {total_results}\n• Затраченное время: {total_time} сек.</b>",
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
