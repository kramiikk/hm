import asyncio
import bisect
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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class BroadcastMessage:
    chat_id: int
    message_id: int
    grouped_id: Optional[int] = None
    album_ids: List[int] = field(default_factory=list)


@dataclass
class BroadcastCode:
    chats: Set[int] = field(default_factory=set)
    messages: List[BroadcastMessage] = field(default_factory=list)
    interval: Tuple[int, int] = field(default_factory=lambda: (1, 13))
    send_mode: str = "auto"

    def validate_interval(self) -> bool:
        return (
            isinstance(self.interval[0], int)
            and isinstance(self.interval[1], int)
            and 0 < self.interval[0] < self.interval[1] <= 1440
        )

    def normalize_interval(self) -> Tuple[int, int]:
        return self.interval if self.validate_interval() else (1, 13)


class BroadcastConfig:
    def __init__(self):
        self.codes: Dict[str, BroadcastCode] = {}
        self._lock = asyncio.Lock()

    async def add_code(self, code_name: str) -> None:
        async with self._lock:
            self.codes.setdefault(code_name, BroadcastCode())

    async def remove_code(self, code_name: str) -> bool:
        async with self._lock:
            return bool(self.codes.pop(code_name, None))


class PrecisionTimer:
    def __init__(self):
        self._target_time: Optional[float] = None
        self._event = asyncio.Event()

    async def wait(self, interval: float):
        self._target_time = time.time() + interval
        self._event.clear()

        try:
            await asyncio.wait_for(self._event.wait(), timeout=interval)
            return False
        except asyncio.TimeoutError:
            return True

    def cancel(self):
        self._event.set()

    @property
    def remaining(self) -> float:
        if self._target_time is None:
            return 0
        return max(0, self._target_time - time.time())


class BroadcastManager:
    def __init__(self, client: TelegramClient, db):
        self.client = client
        self.db = db
        self.config = BroadcastConfig()
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self.message_indices: Dict[str, int] = {}
        self._active = True
        self._last_broadcast_time: Dict[str, float] = {}
        self._scheduled_messages: Dict[str, Set[int]] = {}

    def _load_config_from_dict(self, data: dict):
        for code_name, code_data in data.get("code_chats", {}).items():
            try:
                chats = {int(chat_id) for chat_id in code_data.get("chats", [])}
                messages = [
                    BroadcastMessage(
                        chat_id=int(msg_data["chat_id"]),
                        message_id=int(msg_data["message_id"]),
                        grouped_id=msg_data.get("grouped_id"),
                        album_ids=msg_data.get("album_ids", []),
                    )
                    for msg_data in code_data.get("messages", [])
                ]

                interval = code_data.get("interval", (1, 13))

                broadcast_code = BroadcastCode(
                    chats=chats,
                    messages=messages,
                    interval=tuple(interval),
                    send_mode=code_data.get("send_mode", "auto"),
                )
                self.config.codes[code_name] = broadcast_code
            except Exception as e:
                logger.error(f"Error loading broadcast code {code_name}: {e}")

    def save_config(self):
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
                                "album_ids": msg.album_ids,
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
            logger.error(f"Failed to save config: {e}")

    async def _fetch_messages(
        self, msg_data: BroadcastMessage
    ) -> Union[Message, List[Message], None]:
        try:
            if msg_data.grouped_id is not None:
                messages = [
                    msg
                    for msg_id in msg_data.album_ids
                    if (
                        msg := await self.client.get_messages(
                            msg_data.chat_id, ids=msg_id
                        )
                    )
                ]
                return messages if messages else None
            else:
                return await self.client.get_messages(
                    msg_data.chat_id, ids=msg_data.message_id
                )
        except Exception as e:
            logger.error(f"Failed to fetch message: {e}")
            return None

    async def add_message(self, code_name: str, message: Message) -> bool:
        try:
            await self.config.add_code(code_name)
            code = self.config.codes[code_name]
            grouped_id = getattr(message, "grouped_id", None)

            if grouped_id:
                album_messages = [message]
                async for album_msg in self.client.iter_messages(
                    message.chat_id, limit=10, offset_date=message.date
                ):
                    if (
                        hasattr(album_msg, "grouped_id")
                        and album_msg.grouped_id == message.grouped_id
                    ):
                        album_messages.append(album_msg)
                album_messages.sort(key=lambda m: m.id)

                msg_data = BroadcastMessage(
                    chat_id=message.chat_id,
                    message_id=message.id,
                    grouped_id=grouped_id,
                    album_ids=list(dict.fromkeys([msg.id for msg in album_messages])),
                )
            else:
                msg_data = BroadcastMessage(
                    chat_id=message.chat_id, message_id=message.id
                )
            code.messages.append(msg_data)
            self.save_config()
            return True
        except Exception as e:
            logger.error(f"Failed to add message: {str(e)}")
            return False

    async def _send_message(
        self,
        chat_id: int,
        message_to_send: Union[Message, List[Message]],
        send_mode: str = "auto",
        code_name: Optional[str] = None,
        interval: Optional[float] = None,
    ):
        try:
            code_name = code_name or "default"
            schedule_time = datetime.now() + timedelta(seconds=interval or 60)

            async def _schedule_single_message(entity, message):
                try:
                    if send_mode == "forward" or (
                        send_mode == "auto" and message.media
                    ):
                        return await self.client.forward_messages(
                            entity=entity,
                            messages=[message.id],
                            from_peer=message.chat_id,
                            schedule=schedule_time,
                        )
                    if message.media:
                        return await self.client.send_file(
                            entity=entity,
                            file=message.media,
                            caption=message.text,
                            schedule=schedule_time,
                        )
                    return await self.client.send_message(
                        entity=entity,
                        message=message.text,
                        schedule=schedule_time,
                    )
                except Exception as media_error:
                    logger.error(
                        f"Message scheduling error in chat {entity}: {media_error}"
                    )
                    return await self.client.send_message(
                        entity=entity,
                        message=str(message),
                        schedule=schedule_time,
                    )

            if isinstance(message_to_send, list):
                await self.client.forward_messages(
                    entity=chat_id,
                    messages=[m.id for m in message_to_send],
                    from_peer=message_to_send[0].chat_id,
                    schedule=schedule_time,
                )
            else:
                await _schedule_single_message(chat_id, message_to_send)
        except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
            logger.error(f"Cannot send message to {chat_id}: {e}")
            raise

    async def _broadcast_loop(self, code_name: str):
        precision_timer = PrecisionTimer()
        while self._active:
            await precision_timer.wait(13)
            try:
                code = self.config.codes.get(code_name)
                if not code or not code.chats:
                    continue
                start_time = time.time()
                min_interval, max_interval = code.normalize_interval()
                interval = random.uniform(min_interval * 60, max_interval * 60)
                time_since_last_broadcast = start_time - self._last_broadcast_time.get(
                    code_name, 0
                )
                if time_since_last_broadcast < interval:
                    remaining_time = max(0, interval - time_since_last_broadcast)
                    await precision_timer.wait(remaining_time)
                    continue
                messages = [
                    await self._fetch_messages(msg_data)
                    for msg_data in code.messages
                    if await self._fetch_messages(msg_data)
                ]

                if not messages:
                    continue
                chats = list(code.chats)
                random.shuffle(chats)
                message_index = self.message_indices.get(code_name, 0)
                messages_to_send = messages[message_index % len(messages)]
                self.message_indices[code_name] = (message_index + 1) % len(messages)

                send_mode = getattr(code, "send_mode", "auto")

                failed_chats = set()
                for chat_id in chats:
                    try:
                        await self._send_message(
                            chat_id,
                            messages_to_send,
                            send_mode,
                            code_name,
                            interval,
                        )
                    except Exception as send_error:
                        logger.error(f"Sending error to {chat_id}: {send_error}")
                        failed_chats.add(chat_id)
                if failed_chats:
                    code.chats -= failed_chats
                    self.save_config()
                self._last_broadcast_time[code_name] = time.time()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Critical error in broadcast loop {code_name}: {e}")

    async def start_broadcasts(self):
        for code_name in self.config.codes:
            if code_name not in self.broadcast_tasks:
                try:
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )
                except Exception as e:
                    logger.error(f"Failed to start broadcast loop for {code_name}: {e}")


@loader.tds
class BroadcastMod(loader.Module):

    strings = {
        "name": "Broadcast",
        "code_not_found": "Код рассылки '{}' не найден",
        "success": "Операция выполнена успешно: {}",
        "album_added": "Альбом добавлен в рассылку '{}'",
        "single_added": "Сообщение добавлено в рассылку '{}'",
    }

    def __init__(self):
        self._manager: Optional[BroadcastManager] = None
        self._wat_mode = False
        self._last_broadcast_check: float = 0

    def save_broadcast_status(self):
        broadcast_status = {
            code_name: True for code_name in self._manager.broadcast_tasks
        }
        self._manager.db.set("broadcast", "BroadcastStatus", broadcast_status)

    async def client_ready(self, client: TelegramClient, db: Any):
        self._manager = BroadcastManager(client, db)

        config_data = db.get("broadcast", "Broadcast", {})
        self._manager._load_config_from_dict(config_data)

        broadcast_status = db.get("broadcast", "BroadcastStatus", {})

        for code_name in self._manager.config.codes:
            if broadcast_status.get(code_name, False):
                try:
                    await self._check_and_adjust_message_index(code_name)

                    self._manager.broadcast_tasks[code_name] = asyncio.create_task(
                        self._manager._broadcast_loop(code_name)
                    )
                    logger.info(f"Автоматически восстановлена рассылка: {code_name}")
                except Exception as e:
                    logger.error(f"Не удалось восстановить рассылку {code_name}: {e}")
        self._me_id = client.tg_id

    async def _check_and_adjust_message_index(self, code_name: str):
        logger.info(
            f"Начало проверки запланированных сообщений для кода рассылки: {code_name}"
        )

        try:
            code = self._manager.config.codes.get(code_name)
            if not code or not code.chats:
                logger.info(
                    f"Для кода {code_name} не найдены чаты или код не существует"
                )
                return

            logger.info(f"Найдено чатов для проверки: {len(code.chats)}")

            def check_message_match(original_message, scheduled_message):
                if hasattr(original_message, "media") and hasattr(
                    scheduled_message, "media"
                ):
                    if hasattr(original_message.media, "photo") and hasattr(
                        scheduled_message.media, "photo"
                    ):
                        match = (
                            original_message.media.photo.id
                            == scheduled_message.media.photo.id
                            and original_message.media.photo.access_hash
                            == scheduled_message.media.photo.access_hash
                        )
                        logger.info(f"Сравнение фото: {match}")
                        return match

                    if hasattr(original_message.media, "document") and hasattr(
                        scheduled_message.media, "document"
                    ):
                        match = (
                            original_message.media.document.id
                            == scheduled_message.media.document.id
                            and original_message.media.document.access_hash
                            == scheduled_message.media.document.access_hash
                        )
                        logger.info(f"Сравнение документа: {match}")
                        return match

                text_match = original_message.text == scheduled_message.text
                logger.info(f"Сравнение текста: {text_match}")
                return text_match

            for chat_id in code.chats:
                try:
                    logger.info(
                        f"Проверка запланированных сообщений для чата: {chat_id}"
                    )
                    peer = await self._manager.client.get_input_entity(chat_id)

                    logger.info(
                        f"Получение списка запланированных сообщений для чата {chat_id}"
                    )
                    scheduled_messages = await self._manager.client(
                        functions.messages.GetScheduledHistoryRequest(peer=peer, hash=0)
                    )

                    if not scheduled_messages.messages:
                        logger.info(f"В чате {chat_id} нет запланированных сообщений")
                        continue

                    last_scheduled_messages = sorted(
                        scheduled_messages.messages,
                        key=lambda x: x.date,
                        reverse=True,
                    )

                    logger.info(
                        f"Найдено запланированных сообщений: {len(last_scheduled_messages)}"
                    )

                    for index, msg_data in enumerate(code.messages):
                        logger.info(f"Проверка сообщения индекс {index}")
                        fetch_message = await self._manager._fetch_messages(msg_data)

                        if not fetch_message:
                            logger.info(
                                f"Не удалось получить сообщение для индекса {index}"
                            )
                            continue

                        if isinstance(fetch_message, list):
                            logger.info("Обработка альбома")
                            original_message = fetch_message[0]

                            for scheduled_msg in last_scheduled_messages:
                                if check_message_match(original_message, scheduled_msg):
                                    self._manager.message_indices[code_name] = index
                                    logger.info(
                                        f"✅ Индекс для альбома '{code_name}' установлен на {index}"
                                    )
                                    return

                        else:
                            logger.info("Обработка одиночного сообщения")

                            for scheduled_msg in last_scheduled_messages:
                                if check_message_match(fetch_message, scheduled_msg):
                                    self._manager.message_indices[code_name] = index
                                    logger.info(
                                        f"✅ Индекс для '{code_name}' установлен на {index}"
                                    )
                                    return

                    logger.info(f"Не найдено совпадений для кода рассылки {code_name}")

                except Exception as chat_error:
                    logger.error(
                        f"❌ Ошибка проверки запланированных сообщений в чате {chat_id} "
                        f"для кода {code_name}: {chat_error}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(
                f"❌ Критическая ошибка в проверке запланированных сообщений для {code_name}: {e}",
                exc_info=True,
            )

    async def _validate_broadcast_code(
        self, message: Message, code_name: Optional[str] = None
    ) -> Optional[str]:
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
        reply = await message.get_reply_message()
        if not reply:
            return await utils.answer(
                message, "Ответьте на сообщение командой .addmsg <код>"
            )
        args = utils.get_args(message)
        if len(args) != 1:
            return await utils.answer(message, "Использование: .addmsg <код>")
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
        args = utils.get_args(message)

        if not args:
            if any(self._manager.broadcast_tasks.values()):
                for code_name, task in list(self._manager.broadcast_tasks.items()):
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                    self._manager.broadcast_tasks.pop(code_name, None)
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
                self._manager.db.set("broadcast", "BroadcastStatus", broadcast_status)

                await utils.answer(message, f"Рассылка '{code_name}' остановлена")
            else:
                try:
                    self._manager.broadcast_tasks[code_name] = asyncio.create_task(
                        self._manager._broadcast_loop(code_name)
                    )

                    broadcast_status = self._manager.db.get(
                        "broadcast", "BroadcastStatus", {}
                    )
                    broadcast_status[code_name] = True
                    self._manager.db.set(
                        "broadcast", "BroadcastStatus", broadcast_status
                    )

                    await utils.answer(message, f"Рассылка '{code_name}' запущена")
                except Exception as e:
                    logger.error(f"Failed to start broadcast loop for {code_name}: {e}")
                    await utils.answer(
                        message, f"Не удалось запустить рассылку '{code_name}'"
                    )

    async def chatcmd(self, message: Message):
        args = utils.get_args(message)
        if len(args) != 2:
            return await utils.answer(message, "Использование: .chat <код> <id_чата>")
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

        self._manager.save_config()

        await utils.answer(
            message,
            self.strings["success"].format(f"Код рассылки '{code_name}' удален"),
        )

    async def delmsgcmd(self, message: Message):
        args = utils.get_args(message)
        code_name = await self._validate_broadcast_code(message)
        if not code_name:
            return
        reply = await message.get_reply_message()

        if len(args) == 1 and reply:
            code = self._manager.config.codes[code_name]
            matching_messages = [
                idx
                for idx, msg in enumerate(code.messages)
                if msg.message_id == reply.id and msg.chat_id == reply.chat_id
            ]

            if matching_messages:
                del code.messages[matching_messages[0]]
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
                    self._manager.save_config()
                    await utils.answer(message, "Сообщение удалено")
                else:
                    await utils.answer(message, "Неверный индекс")
            except ValueError:
                await utils.answer(message, "Индекс должен быть числом")

    async def intervalcmd(self, message: Message):
        args = utils.get_args(message)
        if len(args) != 3:
            return await utils.answer(
                message,
                "Использование: .interval <код> <мин_минут> <макс_минут>",
            )
        code_name, min_str, max_str = args
        code_name = await self._validate_broadcast_code(message, code_name)
        if not code_name:
            return
        try:
            min_minutes, max_minutes = map(int, (min_str, max_str))

            code = self._manager.config.codes[code_name]
            code.interval = (min_minutes, max_minutes)

            if not code.validate_interval():
                await utils.answer(message, "Некорректный интервал")
                return
            self._manager.save_config()

            await utils.answer(
                message,
                self.strings["success"].format(
                    f"Интервал для '{code_name}' установлен: {min_minutes}-{max_minutes} минут"
                ),
            )
        except ValueError:
            await utils.answer(message, "Интервал должен быть числом")

    async def listcmd(self, message: Message):
        if not self._manager.config.codes:
            return await utils.answer(message, "Нет настроенных кодов рассылки")
        text = [
            "**Рассылка:**",
            f"🔄 Управление чатами: {'Включено' if self._wat_mode else 'Выключено'}\n",
            "**Коды рассылок:**",
        ]

        for code_name, code in self._manager.config.codes.items():
            chat_list = ", ".join(map(str, code.chats)) or "(пусто)"
            min_interval, max_interval = code.interval
            message_count = len(code.messages)
            running = code_name in self._manager.broadcast_tasks

            text.append(
                f"- `{code_name}`:\n"
                f"  💬 Чаты: {chat_list}\n"
                f"  ⏱ Интервал: {min_interval} - {max_interval} минут\n"
                f"  📨 Сообщений: {message_count}\n"
                f"  📊 Статус: {'🟢 Работает' if running else '🔴 Остановлен'}\n"
            )
        await utils.answer(message, "\n".join(text))

    async def listmsgcmd(self, message: Message):
        code_name = await self._validate_broadcast_code(message)
        if not code_name:
            return
        messages = self._manager.config.codes[code_name].messages
        if not messages:
            return await utils.answer(message, f"Нет сообщений в коде '{code_name}'")
        text = [f"**Сообщения в '{code_name}':**"]
        for i, msg in enumerate(messages, 1):
            try:
                chat_id = int(str(abs(msg.chat_id))[-10:])

                if msg.grouped_id is not None:
                    message_text = f"{i}. Альбом в чате {msg.chat_id} (Всего изображений: {len(msg.album_ids)})"
                    message_links = [
                        f"t.me/c/{chat_id}/{album_id}" for album_id in msg.album_ids
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
        self._wat_mode = not self._wat_mode
        await utils.answer(
            message,
            self.strings["success"].format(
                f"Автоматическое управление чатами {'включено' if self._wat_mode else 'выключено'}"
            ),
        )

    async def watcher(self, message: Message):
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
