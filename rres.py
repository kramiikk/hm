import asyncio
import logging
import random
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Union, Any
from datetime import datetime, timedelta

from telethon import TelegramClient, functions
from telethon.errors import ChatWriteForbiddenError, UserBannedInChannelError
from telethon.tl.types import Message

from .. import loader, utils

logger = logging.getLogger(__name__)


@dataclass(frozen=True)  # Помечаем как immutable, где это возможно
class BroadcastMessage:
    """Сообщение для рассылки."""

    chat_id: int
    message_id: int
    grouped_id: Optional[int] = None
    album_ids: Tuple[int, ...] = field(
        default_factory=tuple
    )  # Используем tuple


@dataclass
class BroadcastCode:
    """Набор настроек для рассылки."""

    chats: Set[int] = field(default_factory=set)
    messages: List[BroadcastMessage] = field(default_factory=list)
    interval: Tuple[int, int] = field(default_factory=lambda: (10, 13))
    send_mode: str = "auto"

    def validate_interval(self) -> bool:
        """Проверяет корректность интервала."""
        min_val, max_val = self.interval
        return (
            isinstance(min_val, int)
            and isinstance(max_val, int)
            and 0 < min_val < max_val <= 1440
        )

    def normalize_interval(self) -> Tuple[int, int]:
        """Нормализует интервал, если он некорректный."""
        return self.interval if self.validate_interval() else (10, 13)


class BroadcastConfig:
    """Управляет конфигурацией рассылки, хранит все коды рассылок."""

    def __init__(self):
        self.codes: Dict[str, BroadcastCode] = {}
        self._lock = asyncio.Lock()

    async def add_code(self, code_name: str) -> None:
        """Добавляет новый код рассылки, если он не существует."""
        async with self._lock:
            if code_name not in self.codes:
                self.codes[code_name] = BroadcastCode()

    async def remove_code(self, code_name: str) -> bool:
        """Удаляет код рассылки."""
        async with self._lock:
            return self.codes.pop(code_name, None) is not None


class BroadcastManager:
    """Управляет рассылками, хранит и обрабатывает информацию о рассылках."""

    def __init__(self, client: TelegramClient, db):
        """Инициализирует менеджер рассылок."""
        self.client = client
        self.db = db
        self.config = BroadcastConfig()
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self.message_indices: Dict[str, int] = {}
        self._active = True
        self._last_broadcast_time: Dict[str, float] = {}
        self._message_cache: Dict[
            Tuple[int, int], Union[Message, List[Message]]
        ] = {}  # Cache fetched messages

    def _load_config_from_dict(self, data: dict):
        """Загружает конфигурацию рассылки из словаря."""
        for code_name, code_data in data.get("code_chats", {}).items():
            try:
                chats = set(code_data.get("chats", []))
                messages = [
                    BroadcastMessage(
                        chat_id=msg_data["chat_id"],
                        message_id=msg_data["message_id"],
                        grouped_id=msg_data.get("grouped_id"),
                        album_ids=tuple(
                            msg_data.get("album_ids", [])
                        ),  # Используем tuple
                    )
                    for msg_data in code_data.get("messages", [])
                ]
                interval = tuple(code_data.get("interval", (10, 13)))
                send_mode = code_data.get("send_mode", "auto")
                broadcast_code = BroadcastCode(
                    chats, messages, interval, send_mode
                )
                self.config.codes[code_name] = broadcast_code
            except Exception as e:
                logger.exception(f"Error loading broadcast code {code_name}")

    def save_config(self):
        """Сохраняет текущую конфигурацию рассылки в базу данных."""
        try:
            config_dict = {
                "code_chats": {
                    code_name: {
                        "chats": list(code.chats),
                        "messages": [
                            {
                                "chat_id": msg.chat_id,
                                "message_id": msg.message_id,
                                "grouped_id": msg.grouped_id,
                                "album_ids": list(
                                    msg.album_ids
                                ),  # Сохраняем как list
                            }
                            for msg in code.messages
                        ],
                        "interval": list(code.interval),
                        "send_mode": code.send_mode,
                    }
                    for code_name, code in self.config.codes.items()
                }
            }
            self.db.set("broadcast", "Broadcast", config_dict)
        except Exception as e:
            logger.exception("Failed to save config")

    async def _fetch_messages(
        self, msg_data: BroadcastMessage
    ) -> Union[Message, List[Message], None]:
        """Получает сообщение или список сообщений из Telegram, используя кэш."""
        cache_key = (msg_data.chat_id, msg_data.message_id)
        if cache_key in self._message_cache:
            return self._message_cache[cache_key]

        try:
            if msg_data.grouped_id is not None:
                messages = await self.client.get_messages(
                    msg_data.chat_id,
                    ids=list(msg_data.album_ids),  # Передаем как list
                )
                if messages:
                    for msg in messages:
                        self._message_cache[(msg_data.chat_id, msg.id)] = msg
                    return messages
                return None
            else:
                message = await self.client.get_messages(
                    msg_data.chat_id, ids=msg_data.message_id
                )
                if message:
                    self._message_cache[cache_key] = message
                    return message
                return None
        except Exception as e:
            logger.exception("Failed to fetch message")
            return None

    async def add_message(self, code_name: str, message: Message) -> bool:
        """Добавляет сообщение в список рассылки."""
        try:
            await self.config.add_code(code_name)
            code = self.config.codes[code_name]
            grouped_id = getattr(message, "grouped_id", None)

            if grouped_id:
                album_messages = [message]
                async for album_msg in self.client.iter_messages(
                    message.chat_id,
                    min_id=message.id,
                ):
                    if (
                        hasattr(album_msg, "grouped_id")
                        and album_msg.grouped_id == message.grouped_id
                        and album_msg.id != message.id
                    ):
                        album_messages.append(album_msg)
                    elif album_msg.id < message.id - 10:  # Safety break
                        break
                album_messages.sort(key=lambda m: m.id)

                unique_album_ids = tuple(msg.id for msg in album_messages)

                msg_data = BroadcastMessage(
                    chat_id=message.chat_id,
                    message_id=message.id,
                    grouped_id=grouped_id,
                    album_ids=unique_album_ids,
                )
            else:
                msg_data = BroadcastMessage(
                    chat_id=message.chat_id, message_id=message.id
                )
            code.messages.append(msg_data)
            self.save_config()
            return True
        except Exception as e:
            logger.exception("Failed to add message")
            return False

    async def _send_message(
        self,
        chat_id: int,
        message_to_send: Union[Message, List[Message]],
        send_mode: str = "auto",
        schedule_time: Optional[datetime] = None,
    ):
        """Отправляет сообщение в указанный чат."""
        try:
            if isinstance(message_to_send, list):
                await self.client.forward_messages(
                    entity=chat_id,
                    messages=[m.id for m in message_to_send],
                    from_peer=message_to_send[0].chat_id,
                    schedule=schedule_time,
                )
            else:
                if send_mode == "forward" or (
                    send_mode == "auto" and message_to_send.media
                ):
                    await self.client.forward_messages(
                        entity=chat_id,
                        messages=[message_to_send.id],
                        from_peer=message_to_send.chat_id,
                        schedule=schedule_time,
                    )
                elif message_to_send.media:
                    await self.client.send_file(
                        entity=chat_id,
                        file=message_to_send.media,
                        caption=message_to_send.text,
                        schedule=schedule_time,
                    )
                else:
                    await self.client.send_message(
                        entity=chat_id,
                        message=message_to_send.text,
                        schedule=schedule_time,
                    )
        except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
            logger.warning(f"Cannot send message to {chat_id}: {e}")
        except Exception as e:
            logger.exception(f"Error sending message to {chat_id}")
            raise

    async def _broadcast_loop(self, code_name: str):
        """Бесконечный цикл рассылки сообщений."""
        while self._active:
            try:
                code = self.config.codes.get(code_name)
                if not code or not code.chats or not code.messages:
                    await asyncio.sleep(60)  # Wait if no config or data
                    continue

                start_time = time.time()
                min_interval, max_interval = code.normalize_interval()
                interval_sec = random.uniform(
                    min_interval * 60, max_interval * 60
                )

                last_broadcast = self._last_broadcast_time.get(code_name, 0)
                if start_time - last_broadcast < interval_sec:
                    await asyncio.sleep(
                        interval_sec - (start_time - last_broadcast)
                    )
                    continue

                messages = [
                    msg
                    for msg in [
                        await self._fetch_messages(msg_data)
                        for msg_data in code.messages
                    ]
                    if msg is not None
                ]
                if not messages:
                    await asyncio.sleep(60)  # Wait if no messages
                    continue

                chats = list(code.chats)
                random.shuffle(chats)
                message_index = self.message_indices.get(code_name, 0)
                message_to_send = messages[message_index % len(messages)]
                self.message_indices[code_name] = (message_index + 1) % len(
                    messages
                )

                send_mode = code.send_mode
                tasks = []
                failed_chats = set()
                for chat_id in chats:
                    task = self._send_message(
                        chat_id, message_to_send, send_mode
                    )
                    tasks.append(task)
                    await asyncio.sleep(
                        random.uniform(1, 3)
                    )  # Reduce max delay

                await asyncio.gather(*tasks, return_exceptions=True)

                for i, chat_id in enumerate(chats):
                    if isinstance(tasks[i], BaseException):
                        failed_chats.add(chat_id)

                if failed_chats:
                    code.chats -= failed_chats
                    self.save_config()

                self._last_broadcast_time[code_name] = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(
                    f"Critical error in broadcast loop {code_name}"
                )
                await asyncio.sleep(60)  # Wait on error

    async def start_broadcasts(self):
        """Запускает все активные рассылки."""
        for code_name in self.config.codes:
            if code_name not in self.broadcast_tasks:
                try:
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )
                except Exception as e:
                    logger.exception(
                        f"Failed to start broadcast loop for {code_name}"
                    )


@loader.tds
class BroadcastMod(loader.Module):
    """Модуль для рассылки сообщений."""

    strings = {
        "name": "Broadcast",
        "code_not_found": "Код рассылки '{}' не найден",
        "success": "Операция выполнена успешно: {}",
        "album_added": "Альбом добавлен в рассылку '{}'",
        "single_added": "Сообщение добавлено в рассылку '{}'",
    }

    def __init__(self):
        """Инициализация модуля."""
        self._manager: Optional[BroadcastManager] = None
        self._wat_mode = False
        self._me_id = None

    def save_broadcast_status(self):
        """Сохраняет статус запущенных рассылок."""
        broadcast_status = {
            code_name: True for code_name in self._manager.broadcast_tasks
        }
        self._manager.db.set("broadcast", "BroadcastStatus", broadcast_status)

    async def client_ready(self, client: TelegramClient, db: Any):
        """Выполняется при готовности клиента Telegram."""
        self._manager = BroadcastManager(client, db)
        self._me_id = client.tg_id

        config_data = db.get("broadcast", "Broadcast", {})
        self._manager._load_config_from_dict(config_data)

        broadcast_status = db.get("broadcast", "BroadcastStatus", {})

        for code_name in self._manager.config.codes:
            if (
                broadcast_status.get(code_name, False)
                and code_name not in self._manager.broadcast_tasks
            ):
                try:
                    await self._check_and_adjust_message_index(code_name)
                    self._manager.broadcast_tasks[code_name] = (
                        asyncio.create_task(
                            self._manager._broadcast_loop(code_name)
                        )
                    )
                except Exception as e:
                    logger.exception(
                        f"Не удалось восстановить рассылку {code_name}"
                    )

    async def _check_and_adjust_message_index(self, code_name: str):
        """Проверяет и корректирует индекс сообщения для рассылки."""
        code = self._manager.config.codes.get(code_name)
        if not code or not code.chats:
            return

        chats_list = list(code.chats)
        if chats_list:
            chat_id = random.choice(chats_list)
            try:
                peer = await self._manager.client.get_input_entity(chat_id)
                scheduled_messages = await self._manager.client(
                    functions.messages.GetScheduledHistoryRequest(
                        peer=peer, hash=0
                    )
                )

                if not scheduled_messages.messages:
                    return

                for index, msg_data in enumerate(code.messages):
                    original_message = await self._manager._fetch_messages(
                        msg_data
                    )
                    if not original_message:
                        continue

                    def match_messages(orig, scheduled):
                        if orig.media and scheduled.media:
                            if hasattr(orig.media, "photo") and hasattr(
                                scheduled.media, "photo"
                            ):
                                return (
                                    orig.media.photo.id
                                    == scheduled.media.photo.id
                                )
                            if hasattr(orig.media, "document") and hasattr(
                                scheduled.media, "document"
                            ):
                                return (
                                    orig.media.document.id
                                    == scheduled.media.document.id
                                )
                        return orig.text == scheduled.message

                    if isinstance(original_message, list):
                        first_original_msg = original_message[0]
                        match = next(
                            (
                                msg
                                for msg in scheduled_messages.messages
                                if hasattr(msg, "grouped_id")
                                and match_messages(first_original_msg, msg)
                            ),
                            None,
                        )
                    else:
                        match = next(
                            (
                                msg
                                for msg in scheduled_messages.messages
                                if match_messages(original_message, msg)
                            ),
                            None,
                        )

                    if match:
                        self._manager.message_indices[code_name] = index
                        if hasattr(match, "date"):
                            self._manager._last_broadcast_time[code_name] = (
                                match.date.timestamp()
                            )
                        return
            except Exception as e:
                logger.warning(
                    f"Error checking scheduled messages for {code_name} in chat {chat_id}: {e}"
                )

    async def _validate_broadcast_code(
        self, message: Message, code_name: Optional[str] = None
    ) -> Optional[str]:
        """Проверяет существование кода рассылки."""
        args = utils.get_args(message)

        if code_name is None:
            if not args:
                await utils.answer(message, "Укажите код рассылки")
                return None
            code_name = args[0]
        if code_name not in self._manager.config.codes:
            await utils.answer(
                message, self.strings["code_not_found"].format(code_name)
            )
            return None
        return code_name

    async def addmsgcmd(self, message: Message):
        """Команда добавления сообщения в рассылку. Использование: .addmsg кодовое_слово
        Нужно ответить на сообщение.
        """
        reply = await message.get_reply_message()
        if not reply:
            return await utils.answer(
                message, "Ответьте на сообщение командой .addmsg кодовое_слово"
            )
        args = utils.get_args(message)
        if len(args) != 1:
            return await utils.answer(
                message, "Использование: .addmsg кодовое_слово"
            )
        code_name = args[0]
        success = await self._manager.add_message(code_name, reply)

        if success:
            await utils.answer(
                message,
                (
                    self.strings["album_added"].format(code_name)
                    if getattr(reply, "grouped_id", None)
                    else self.strings["single_added"].format(code_name)
                ),
            )
        else:
            await utils.answer(message, "Не удалось добавить сообщение")

    async def broadcastcmd(self, message: Message):
        """Команда управления рассылкой.
        Использование:
            .broadcast - Запускает/останавливает все рассылки
            .broadcast кодовое_слово - только конкретную рассылку
        """
        args = utils.get_args(message)

        if not args:
            if self._manager.broadcast_tasks:
                for code_name, task in list(
                    self._manager.broadcast_tasks.items()
                ):
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                self._manager.broadcast_tasks.clear()
                self._manager.db.set("broadcast", "BroadcastStatus", {})
                await utils.answer(message, "Все рассылки остановлены")
            else:
                await self._manager.start_broadcasts()
                self.save_broadcast_status()
                await utils.answer(message, "Все рассылки запущены")
        else:
            code_name = args[0]
            if code_name not in self._manager.config.codes:
                return await utils.answer(
                    message, self.strings["code_not_found"].format(code_name)
                )
            if code_name in self._manager.broadcast_tasks:
                task = self._manager.broadcast_tasks.pop(code_name)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
                broadcast_status = self._manager.db.get(
                    "broadcast", "BroadcastStatus", {}
                )
                broadcast_status.pop(code_name, None)
                self._manager.db.set(
                    "broadcast", "BroadcastStatus", broadcast_status
                )
                await utils.answer(
                    message, f"Рассылка '{code_name}' остановлена"
                )
            else:
                try:
                    self._manager.broadcast_tasks[code_name] = (
                        asyncio.create_task(
                            self._manager._broadcast_loop(code_name)
                        )
                    )
                    broadcast_status = self._manager.db.get(
                        "broadcast", "BroadcastStatus", {}
                    )
                    broadcast_status[code_name] = True
                    self._manager.db.set(
                        "broadcast", "BroadcastStatus", broadcast_status
                    )
                    await utils.answer(
                        message, f"Рассылка '{code_name}' запущена"
                    )
                except Exception as e:
                    logger.exception(
                        f"Failed to start broadcast loop for {code_name}"
                    )
                    await utils.answer(
                        message, f"Не удалось запустить рассылку '{code_name}'"
                    )

    async def chatcmd(self, message: Message):
        """Команда добавления/удаления чата из рассылки.
        Использование: .chat кодовое_слово id_чата
        """
        args = utils.get_args(message)
        if len(args) != 2:
            return await utils.answer(
                message, "Использование: .chat кодовое_слово id_чата"
            )
        try:
            code_name, chat_id = args[0], int(args[1])
        except ValueError:
            return await utils.answer(message, "ID чата должен быть числом")
        code_name = await self._validate_broadcast_code(message, code_name)
        if not code_name:
            return
        try:
            code = self._manager.config.codes[code_name]
            if chat_id in code.chats:
                code.chats.remove(chat_id)
                action = "удален"
            else:
                code.chats.add(chat_id)
                action = "добавлен"
            self._manager.save_config()
            await utils.answer(message, f"Чат {chat_id} {action} в {code_name}")
        except Exception as e:
            await utils.answer(message, f"Ошибка: {str(e)}")

    async def delcodecmd(self, message: Message):
        """Команда удаления кода рассылки.
        Использование: .delcode кодовое_слово
        """
        code_name = await self._validate_broadcast_code(message)
        if not code_name:
            return
        if code_name in self._manager.broadcast_tasks:
            task = self._manager.broadcast_tasks.pop(code_name)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await self._manager.config.remove_code(code_name)
        self._manager.message_indices.pop(code_name, None)
        self._manager._message_cache.clear()
        self._manager.save_config()
        await utils.answer(
            message,
            self.strings["success"].format(
                f"Код рассылки '{code_name}' удален"
            ),
        )

    async def delmsgcmd(self, message: Message):
        """Команда удаления сообщения из рассылки.
        Использование:
            .delmsg кодовое_слово - Удаляет сообщение, на которое ответили
            .delmsg кодовое_слово индекс - Удаляет сообщение по индексу из списка
        """
        args = utils.get_args(message)
        code_name = await self._validate_broadcast_code(message)
        if not code_name:
            return
        reply = await message.get_reply_message()

        if len(args) == 1 and reply:
            code = self._manager.config.codes[code_name]
            initial_len = len(code.messages)
            code.messages = [
                msg
                for msg in code.messages
                if not (
                    msg.message_id == reply.id and msg.chat_id == reply.chat_id
                )
            ]
            if len(code.messages) < initial_len:
                self._manager._message_cache.clear()
                self._manager.save_config()
                await utils.answer(message, "Сообщение удалено")
            else:
                await utils.answer(message, "Сообщение не найдено")
        elif len(args) == 2:
            try:
                index = int(args[1]) - 1
                code = self._manager.config.codes[code_name]
                if 0 <= index < len(code.messages):
                    del code.messages[index]
                    self._manager._message_cache.clear()
                    self._manager.save_config()
                    await utils.answer(message, "Сообщение удалено")
                else:
                    await utils.answer(message, "Неверный индекс")
            except ValueError:
                await utils.answer(message, "Индекс должен быть числом")

    async def intervalcmd(self, message: Message):
        """Команда изменения интервала рассылки.
        Использование: .interval кодовое_слово мин_минут макс_минут
        """
        args = utils.get_args(message)
        if len(args) != 3:
            return await utils.answer(
                message,
                "Использование: .interval кодовое_слово мин_минут макс_минут",
            )
        code_name, min_str, max_str = args
        code_name = await self._validate_broadcast_code(message, code_name)
        if not code_name:
            return
        try:
            min_minutes, max_minutes = map(int, (min_str, max_str))
        except ValueError:
            return await utils.answer(message, "Интервал должен быть числом")

        code = self._manager.config.codes[code_name]
        code.interval = (min_minutes, max_minutes)

        if not code.validate_interval():
            return await utils.answer(
                message,
                "Некорректный интервал. Убедитесь, что минимальное значение 1, а максимальное 1440.",
            )

        self._manager.save_config()
        await utils.answer(
            message,
            self.strings["success"].format(
                f"Интервал для '{code_name}' установлен: {min_minutes}-{max_minutes} минут"
            ),
        )

    async def listcmd(self, message: Message):
        """Команда для получения списка кодов рассылок и их статусов."""
        if not self._manager.config.codes:
            return await utils.answer(message, "Нет настроенных кодов рассылки")
        text = [
            "<b>Рассылка:</b>",
            f"🔄 Управление чатами: {'Включено' if self._wat_mode else 'Выключено'}\n",
            "<b>Коды рассылок:</b>",
        ]

        for code_name, code in self._manager.config.codes.items():
            chat_list = ", ".join(map(str, code.chats)) or "(пусто)"
            min_interval, max_interval = code.interval
            message_count = len(code.messages)
            running = code_name in self._manager.broadcast_tasks

            text.append(
                f"+ <code>{code_name}</code>:\n"
                f"  💬 Чаты: {chat_list}\n"
                f"  ⏱ Интервал: {min_interval} - {max_interval} минут\n"
                f"  📨 Сообщений: {message_count}\n"
                f"  📊 Статус: {'🟢 Работает' if running else '🔴 Остановлен'}\n"
            )
        await utils.answer(message, "\n".join(text))

    async def listmsgcmd(self, message: Message):
        """
        Команда для просмотра списка сообщений в определенном коде рассылки.

        Использование: .listmsg кодовое_слово
        """
        code_name = await self._validate_broadcast_code(message)
        if not code_name:
            return
        messages = self._manager.config.codes[code_name].messages
        if not messages:
            return await utils.answer(
                message, f"Нет сообщений в коде '{code_name}'"
            )
        text = [f"<b>Сообщения в '{code_name}':</b>"]
        for i, msg in enumerate(messages, 1):
            try:
                chat_id = int(str(abs(msg.chat_id))[-10:])

                if msg.grouped_id is not None:
                    message_text = f"{i}. Альбом в чате {msg.chat_id} (Всего изображений: {len(msg.album_ids)})"
                    message_links = [
                        f"t.me/c/{chat_id}/{album_id}"
                        for album_id in msg.album_ids
                    ]
                    message_text += f"\nСсылки: {' , '.join(message_links)}"
                else:
                    message_text = f"{i}. Сообщение в чате {msg.chat_id}\n"
                    message_text += f"Ссылка: t.me/c/{chat_id}/{msg.message_id}"
                text.append(message_text)
            except Exception as e:
                text.append(f"{i}. Ошибка получения информации: {str(e)}")
        await utils.answer(message, "\n\n".join(text))

    async def sendmodecmd(self, message: Message):
        """
        Команда для изменения режима отправки сообщений.

        Использование: .sendmode кодовое_слово режим

        - Режим отправки:
            - auto: Автоматический выбор (по умолчанию).
            - normal: Обычная отправка текста.
            - forward: Пересылка сообщения.
        """
        args = utils.get_args(message)
        if len(args) != 2 or args[1] not in ["auto", "normal", "forward"]:
            return await utils.answer(
                message,
                "Использование: .sendmode <код> <режим>\n"
                "Режимы: auto (по умолчанию), normal (обычная отправка), forward (форвард)",
            )
        code_name, mode = args
        code_name = await self._validate_broadcast_code(message, code_name)
        if not code_name:
            return
        code = self._manager.config.codes[code_name]
        code.send_mode = mode
        self._manager.save_config()

        await utils.answer(
            message,
            self.strings["success"].format(
                f"Режим отправки для '{code_name}' установлен: {mode}"
            ),
        )

    async def watcmd(self, message: Message):
        """
        Команда для включения/выключения автоматического добавления чатов в рассылку.

        Использование: .wat
        """
        self._wat_mode = not self._wat_mode
        await utils.answer(
            message,
            self.strings["success"].format(
                f"Автоматическое управление чатами {'включено' if self._wat_mode else 'выключено'}"
            ),
        )

    async def watcher(self, message: Message):
        """Автоматически добавляет чаты в рассылку, если режим автоматического управления чатами включен."""
        if not isinstance(message, Message) or not self._wat_mode:
            return
        if message.sender_id == self._me_id and message.text:
            for code_name in self._manager.config.codes:
                if message.text.strip().endswith(code_name):
                    try:
                        code = self._manager.config.codes[code_name]
                        code.chats.add(message.chat_id)
                        self._manager.save_config()
                        break
                    except Exception as e:
                        logger.error(f"Ошибка автодобавления чата: {e}")
