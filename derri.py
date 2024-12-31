""" Author: @kramiikk """

import asyncio
import json
import logging
import random
import time
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Union

from telethon.tl.types import Message
from telethon.errors import (
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    ChannelPrivateError,
    ChatAdminRequiredError,
)

from .. import loader

logger = logging.getLogger(__name__)


class SimpleCache:
    """Простой кэш с TTL и ограничением размера"""

    def __init__(self, ttl: int = 3600, max_size: int = 50):
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()
        self._cleanup_interval = 3000

    async def get(self, key):
        """Получает значение из кэша"""
        async with self._lock:
            await self._maybe_cleanup()
            if key not in self.cache:
                return None
            timestamp, value = self.cache[key]
            if time.time() - timestamp > self.ttl:
                del self.cache[key]
                return None
            self.cache.move_to_end(key)
            return value

    async def set(self, key, value):
        """Устанавливает значение в кэш"""
        async with self._lock:
            await self._maybe_cleanup()
            if len(self.cache) >= self.max_size:
                to_remove = max(1, len(self.cache) // 4)
                for _ in range(to_remove):
                    self.cache.popitem(last=False)
            self.cache[key] = (time.time(), value)

    async def _maybe_cleanup(self):
        """Проверяет необходимость очистки устаревших записей"""
        current_time = time.time()
        if current_time - self._last_cleanup > self._cleanup_interval:
            await self.clean_expired()
            self._last_cleanup = current_time

    async def clean_expired(self):
        """Очищает устаревшие записи"""
        async with self._lock:
            current_time = time.time()
            self.cache = OrderedDict(
                (k, v)
                for k, v in self.cache.items()
                if current_time - v[0] <= self.ttl
            )


def register(cb):
    BroadcastMod()._register(cb)
    return []


@loader.tds
class BroadcastMod(loader.Module):
    """Модуль для управления рассылками

    Команды:
    • .broadcast create <код> - создать рассылку
    • .broadcast delete <код> - удалить рассылку
    • .broadcast add <код> - добавить сообщение (ответом)
    • .broadcast remove <код> - удалить сообщение (ответом)
    • .broadcast addchat <код> - добавить текущий чат
    • .broadcast rmchat <код> - удалить текущий чат
    • .broadcast int <код> <мин> <макс> - установить интервал
    • .broadcast mode <код> <режим> - установить режим (auto/normal/schedule)
    • .broadcast batch <код> <on/off> - включить/выключить пакетный режим
    • .broadcast start <код> - запустить рассылку
    • .broadcast stop <код> - остановить рассылку
    • .broadcast list - список рассылок
    """

    strings = {"name": "Broadcast"}

    async def client_ready(self):
        """Инициализация при загрузке модуля"""
        self.manager = BroadcastManager(self._client, self.db)
        self.manager.me_id = (await self._client.get_me()).id

    async def broadcastcmd(self, message):
        """Команда для управления рассылкой. Используйте .help Broadcast для справки."""
        if not self.manager.is_authorized(message.sender_id):
            await message.reply("❌ У вас нет доступа к этой команде")
            return
        await self.manager.handle_command(message)


@dataclass
class Broadcast:
    """Основной класс для управления рассылкой"""

    chats: Set[int] = field(default_factory=set)
    messages: List[dict] = field(default_factory=list)
    interval: Tuple[int, int] = (10, 13)
    send_mode: str = "auto"
    batch_mode: bool = False
    _last_message_index: int = field(default=0, init=False)
    _active: bool = field(default=True, init=False)

    def add_message(
        self, chat_id: int, message_id: int, grouped_ids: List[int] = None
    ) -> bool:
        """Добавляет сообщение с проверкой дубликатов"""
        message_data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "grouped_ids": grouped_ids or [],
        }

        for existing in self.messages:
            if (
                existing["chat_id"] == chat_id
                and existing["message_id"] == message_id
            ):
                return False

        self.messages.append(message_data)
        return True

    def remove_message(self, message_id: int, chat_id: int) -> bool:
        """Удаляет сообщение из списка"""
        initial_length = len(self.messages)
        self.messages = [
            m
            for m in self.messages
            if not (m["message_id"] == message_id and m["chat_id"] == chat_id)
        ]
        return len(self.messages) < initial_length

    def get_next_message_index(self) -> int:
        """Возвращает индекс следующего сообщения для отправки"""
        if not self.messages:
            return 0
        self._last_message_index = (self._last_message_index + 1) % len(
            self.messages
        )
        return self._last_message_index

    def is_valid_interval(self) -> bool:
        """Проверяет корректность интервала"""
        min_val, max_val = self.interval
        return (
            isinstance(min_val, int)
            and isinstance(max_val, int)
            and 0 < min_val < max_val <= 1440
        )

    def normalize_interval(self) -> Tuple[int, int]:
        """Нормализует интервал рассылки"""
        if self.is_valid_interval():
            return self.interval
        return (10, 13)

    def get_random_delay(self) -> int:
        """Возвращает случайную задержку в пределах интервала"""
        min_val, max_val = self.normalize_interval()
        return random.randint(min_val * 60, max_val * 60)

    def to_dict(self) -> dict:
        """Сериализует объект в словарь"""
        return {
            "chats": list(self.chats),
            "messages": self.messages,
            "interval": list(self.interval),
            "send_mode": self.send_mode,
            "batch_mode": self.batch_mode,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Broadcast":
        """Создает объект из словаря"""
        return cls(
            chats=set(data.get("chats", [])),
            messages=data.get("messages", []),
            interval=tuple(data.get("interval", (10, 13))),
            send_mode=data.get("send_mode", "auto"),
            batch_mode=data.get("batch_mode", False),
        )


class BroadcastManager:
    """Manages broadcast operations and state."""

    MAX_MESSAGES_PER_CODE = 100
    MAX_CHATS_PER_CODE = 1000
    MAX_CODES = 50

    def __init__(self, client, db):
        self.client = client
        self.db = db
        self.codes: Dict[str, Broadcast] = {}
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self.last_broadcast_time: Dict[str, float] = {}
        self._message_cache = SimpleCache(ttl=7200, max_size=50)
        self._active = True
        self._lock = asyncio.Lock()
        self.me_id = None
        self._cleanup_task = None
        self._periodic_task = None
        self._authorized_users = self._load_authorized_users()
        self.watcher_enabled = False
        self._load_config()

    def _load_authorized_users(self) -> Set[int]:
        """Загружает список авторизованных пользователей из JSON файла"""
        try:
            with open("/root/Heroku/loll.json", "r") as f:
                data = json.load(f)
                return set(int(uid) for uid in data.get("authorized_users", []))
        except Exception as e:
            logger.error(f"Ошибка загрузки авторизованных пользователей: {e}")
            return {7175372340}

    def is_authorized(self, user_id: int) -> bool:
        """Проверяет, авторизован ли пользователь"""
        return user_id in self._authorized_users or user_id == self.me_id

    async def _save_authorized_users(self):
        """Сохраняет список авторизованных пользователей в JSON файл"""
        try:
            with open("/root/Heroku/loll.json", "r") as f:
                data = json.load(f)
            data["authorized_users"] = list(self._authorized_users)
            with open("/root/Heroku/loll.json", "w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Ошибка сохранения авторизованных пользователей: {e}")

    async def _load_config(self):
        """Загружает конфигурацию из базы данных"""
        try:
            config = self.db.get("broadcast", "config", {})
            if not config:
                return

            for code_name, code_data in config.get("codes", {}).items():
                self.codes[code_name] = Broadcast.from_dict(code_data)

            saved_times = self.db.get("broadcast", "last_broadcast_times", {})
            self.last_broadcast_time.update(
                {code: float(time_) for code, time_ in saved_times.items()}
            )

            self.me_id = (await self.client.get_me()).id

        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")

    async def save_config(self):
        """Сохраняет конфигурацию в базу данных"""
        async with self._lock:
            try:
                config = {
                    "version": 1,
                    "last_save": int(time.time()),
                    "codes": {
                        name: code.to_dict()
                        for name, code in self.codes.items()
                    },
                }
                self.db.set("broadcast", "config", config)
                self.db.set(
                    "broadcast",
                    "last_broadcast_times",
                    self.last_broadcast_time,
                )
            except Exception as e:
                logger.error(f"Ошибка сохранения конфигурации: {e}")

    async def handle_command(self, message: Message):
        """Обработчик команд для управления рассылкой"""
        try:
            args = message.text.split()[1:]
            if not args:
                await message.reply("❌ Укажите действие и код рассылки")
                return

            action = args[0].lower()

            code_name = args[1] if len(args) > 1 else None

            if action == "create":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} уже существует"
                    )
                    return
                self.codes[code_name] = Broadcast()
                await self.save_config()
                await message.reply(f"✅ Рассылка {code_name} создана")

            elif action == "delete":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return
                if (
                    code_name in self.broadcast_tasks
                    and not self.broadcast_tasks[code_name].done()
                ):
                    self.broadcast_tasks[code_name].cancel()
                del self.codes[code_name]
                await self.save_config()
                await message.reply(f"✅ Рассылка {code_name} удалена")

            elif action == "add":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                reply = await message.get_reply_message()
                if not reply:
                    await message.reply(
                        "❌ Ответьте на сообщение, которое нужно добавить в рассылку"
                    )
                    return

                code = self.codes[code_name]
                if len(code.messages) >= self.MAX_MESSAGES_PER_CODE:
                    await message.reply(
                        f"❌ Достигнут лимит сообщений ({self.MAX_MESSAGES_PER_CODE})"
                    )
                    return

                grouped_id = getattr(reply, "grouped_id", None)
                grouped_ids = []

                if grouped_id:
                    async for msg in message.client.iter_messages(
                        reply.chat_id,
                        min_id=max(0, reply.id - 10),
                        max_id=reply.id + 10,
                    ):
                        if getattr(msg, "grouped_id", None) == grouped_id:
                            grouped_ids.append(msg.id)

                if code.add_message(reply.chat_id, reply.id, grouped_ids):
                    await self.save_config()
                    await message.reply("✅ Сообщение добавлено в рассылку")
                else:
                    await message.reply("❌ Это сообщение уже есть в рассылке")

            elif action == "remove":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                reply = await message.get_reply_message()
                if not reply:
                    await message.reply(
                        "❌ Ответьте на сообщение, которое нужно удалить из рассылки"
                    )
                    return

                code = self.codes[code_name]
                if code.remove_message(reply.id, reply.chat_id):
                    await self.save_config()
                    await message.reply("✅ Сообщение удалено из рассылки")
                else:
                    await message.reply(
                        "❌ Это сообщение не найдено в рассылке"
                    )

            elif action == "addchat":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                code = self.codes[code_name]

                if len(args) > 2:
                    chat_id = await self._get_chat_id(args[2])
                    if not chat_id:
                        await message.reply(
                            "❌ Не удалось получить ID чата. Проверьте ссылку/юзернейм"
                        )
                        return
                else:
                    chat_id = message.chat_id

                if len(code.chats) >= self.MAX_CHATS_PER_CODE:
                    await message.reply(
                        f"❌ Достигнут лимит чатов ({self.MAX_CHATS_PER_CODE})"
                    )
                    return

                if chat_id in code.chats:
                    await message.reply("❌ Этот чат уже добавлен в рассылку")
                    return

                code.chats.add(chat_id)
                await self.save_config()
                await message.reply("✅ Чат добавлен в рассылку")

            elif action == "rmchat":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                code = self.codes[code_name]
                chat_id = None

                if len(args) > 2:
                    chat_id = await self._get_chat_id(args[2])
                    if not chat_id:
                        await message.reply(
                            "❌ Не удалось получить ID чата. Проверьте ссылку/юзернейм"
                        )
                        return
                else:
                    chat_id = message.chat_id

                if chat_id not in code.chats:
                    await message.reply("❌ Этот чат не найден в рассылке")
                    return

                code.chats.remove(chat_id)
                await self.save_config()
                await message.reply("✅ Чат удален из рассылки")

            elif action == "int":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                if len(args) < 4:
                    await message.reply(
                        "❌ Укажите минимальный и максимальный интервал в минутах"
                    )
                    return

                try:
                    min_val = int(args[2])
                    max_val = int(args[3])
                except ValueError:
                    await message.reply("❌ Интервалы должны быть числами")
                    return

                code = self.codes[code_name]
                code.interval = (min_val, max_val)

                if not code.is_valid_interval():
                    await message.reply(
                        "❌ Некорректный интервал (0 < min < max <= 1440)"
                    )
                    return

                await self.save_config()
                await message.reply(
                    f"✅ Установлен интервал {min_val}-{max_val} минут"
                )

            elif action == "mode":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                if len(args) < 3:
                    await message.reply(
                        "❌ Укажите режим отправки (auto/normal/schedule)"
                    )
                    return

                mode = args[2].lower()
                if mode not in ["auto", "normal", "schedule"]:
                    await message.reply(
                        "❌ Неверный режим. Доступные режимы: auto, normal, schedule"
                    )
                    return

                code = self.codes[code_name]
                code.send_mode = mode
                await self.save_config()
                await message.reply(f"✅ Установлен режим отправки: {mode}")

            elif action == "batch":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return

                if len(args) < 3:
                    await message.reply("❌ Укажите режим (on/off)")
                    return

                mode = args[2].lower()
                if mode not in ["on", "off"]:
                    await message.reply("❌ Укажите on или off")
                    return

                code = self.codes[code_name]
                code.batch_mode = mode == "on"
                await self.save_config()
                await message.reply(
                    f"✅ Пакетный режим {'включен' if code.batch_mode else 'выключен'}"
                )

            elif action == "start":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return
                self.codes[code_name]._active = True
                if (
                    code_name not in self.broadcast_tasks
                    or self.broadcast_tasks[code_name].done()
                ):
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )
                await message.reply(f"✅ Рассылка {code_name} запущена")

            elif action == "stop":
                if not code_name:
                    await message.reply("❌ Укажите код рассылки")
                    return
                if code_name not in self.codes:
                    await message.reply(
                        f"❌ Код рассылки {code_name} не найден"
                    )
                    return
                self.codes[code_name]._active = False
                if (
                    code_name in self.broadcast_tasks
                    and not self.broadcast_tasks[code_name].done()
                ):
                    self.broadcast_tasks[code_name].cancel()
                await message.reply(f"✅ Рассылка {code_name} остановлена")

            elif action == "watcher":
                if len(args) < 2:
                    status = "включен" if self.watcher_enabled else "выключен"
                    await message.reply(
                        f"❌ Укажите on/off\nТекущий статус: {status}"
                    )
                    return

                mode = args[1].lower()
                if mode not in ["on", "off"]:
                    await message.reply("❌ Укажите on или off")
                    return

                self.watcher_enabled = mode == "on"
                await message.reply(
                    f"✅ Автодобавление чатов {'включено' if self.watcher_enabled else 'выключено'}"
                )

            elif action == "list":
                if not self.codes:
                    await message.reply("❌ Нет активных рассылок")
                    return
                response = "📝 Список рассылок:\n\n"
                for name, code in self.codes.items():
                    status = "✅ Активна" if code._active else "❌ Остановлена"
                    response += f"• {name}: {status}\n"
                    response += f"  - Чатов: {len(code.chats)}\n"
                    response += f"  - Сообщений: {len(code.messages)}\n"
                    response += f"  - Интервал: {code.interval[0]}-{code.interval[1]} мин\n"
                    response += f"  - Режим: {code.send_mode}\n"
                    response += f"  - Пакетный режим: {'включен' if code.batch_mode else 'выключен'}\n"
                await message.reply(response)

            else:
                await message.reply(
                    "❌ Неизвестное действие\n\n"
                    "Доступные команды:\n"
                    "• create <код> - создать рассылку\n"
                    "• delete <код> - удалить рассылку\n"
                    "• add <код> - добавить сообщение (ответом)\n"
                    "• remove <код> - удалить сообщение (ответом)\n"
                    "• addchat <код> - добавить текущий чат\n"
                    "• rmchat <код> - удалить текущий чат\n"
                    "• int <код> <мин> <макс> - установить интервал\n"
                    "• mode <код> <режим> - установить режим (auto/normal/schedule)\n"
                    "• batch <код> <on/off> - включить/выключить пакетный режим\n"
                    "• start <код> - запустить рассылку\n"
                    "• stop <код> - остановить рассылку\n"
                    "• watcher <on/off> - включить/выключить автодобавление чатов\n"
                    "• list - список рассылок"
                )

        except Exception as e:
            logger.error(f"Error handling command: {e}")
            await message.reply(f"❌ Произошла ошибка: {str(e)}")

    async def _send_messages_to_chats(
        self,
        code: Broadcast,
        code_name: str,
        messages_to_send: List[Union[Message, List[Message]]],
    ) -> Set[int]:
        """Отправляет сообщения в чаты с оптимизированной обработкой ошибок."""
        failed_chats = set()
        success_count = 0
        error_counts = {}

        async def send_to_chat(chat_id: int):
            nonlocal success_count

            if not self._active or not code._active:
                return

            try:
                schedule_time = None
                if code.send_mode == "schedule":
                    delay = code.get_random_delay()
                    schedule_time = datetime.now() + timedelta(seconds=delay)

                for message in messages_to_send:
                    success = await self._send_message(
                        code_name,
                        chat_id,
                        message,
                        code.send_mode,
                        schedule_time,
                    )
                    if not success:
                        raise Exception("Ошибка отправки сообщения")

                success_count += 1

            except (
                ChatWriteForbiddenError,
                UserBannedInChannelError,
                ChannelPrivateError,
                ChatAdminRequiredError,
            ) as e:
                error_type = type(e).__name__
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
                failed_chats.add(chat_id)
                logger.warning(f"Ошибка доступа для чата {chat_id}: {str(e)}")

            except Exception as e:
                error_type = type(e).__name__
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
                failed_chats.add(chat_id)
                logger.error(
                    f"Непредвиденная ошибка для чата {chat_id}: {str(e)}"
                )

        batch_size = 10
        chats = list(code.chats)
        random.shuffle(chats)

        for i in range(0, len(chats), batch_size):
            if not self._active or not code._active:
                break

            batch = chats[i : i + batch_size]
            tasks = [send_to_chat(chat_id) for chat_id in batch]
            await asyncio.gather(*tasks)

            await asyncio.sleep(2)

        total_chats = len(code.chats)
        if total_chats > 0:
            success_rate = (success_count / total_chats) * 100
            logger.info(
                f"Рассылка завершена. Успешно: {success_count}/{total_chats} ({success_rate:.1f}%)"
            )
            if error_counts:
                logger.info("Статистика ошибок:")
                for error_type, count in error_counts.items():
                    logger.info(f"- {error_type}: {count}")

        return failed_chats

    async def _send_message(
        self,
        code_name: str,
        chat_id: int,
        messages_to_send: Union[Message, List[Message]],
        send_mode: str = "auto",
        schedule_time: Optional[datetime] = None,
    ) -> bool:
        """Отправляет сообщение с ограничением частоты."""
        try:
            if isinstance(messages_to_send, list):
                messages = messages_to_send
                from_peer = messages[0].chat_id
            elif messages_to_send.media and send_mode != "normal":
                messages = [messages_to_send]
                from_peer = messages_to_send.chat_id
            else:
                await self.client.send_message(
                    entity=chat_id,
                    message=messages_to_send.text,
                    schedule=schedule_time,
                )
                return True

            await self.client.forward_messages(
                entity=chat_id,
                messages=messages,
                from_peer=from_peer,
                schedule=schedule_time,
            )
            return True
        except (ChatWriteForbiddenError, UserBannedInChannelError):
            raise
        except Exception as e:
            logger.error(
                f"Error sending message to {chat_id} in broadcast '{code_name}': {e}"
            )
            return False

    async def _apply_interval(self, code: Broadcast, code_name: str):
        """Применяет интервал между отправками с адаптивной задержкой."""
        try:
            min_interval, max_interval = code.normalize_interval()
            last_broadcast = self.last_broadcast_time.get(code_name, 0)
            current_time = time.time()

            base_interval = code.get_random_delay()

            active_broadcasts = len(
                [t for t in self.broadcast_tasks.values() if not t.done()]
            )
            if active_broadcasts > 5:
                base_interval *= 1.2

            time_since_last = current_time - last_broadcast
            if time_since_last < base_interval:
                sleep_time = base_interval - time_since_last
                await asyncio.sleep(sleep_time)

        except Exception as e:
            logger.error(f"Ошибка применения интервала для {code_name}: {e}")
            await asyncio.sleep(60)

    async def _handle_failed_chats(
        self, code_name: str, failed_chats: Set[int]
    ):
        """Обрабатывает чаты, в которые не удалось отправить сообщения."""
        if not failed_chats:
            return

        try:
            async with self._lock:
                code = self.codes[code_name]
                code.chats -= failed_chats
                await self.save_config()

                chat_groups = []
                current_group = []

                for chat_id in failed_chats:
                    current_group.append(str(chat_id))
                    if len(current_group) >= 50:
                        chat_groups.append(", ".join(current_group))
                        current_group = []

                if current_group:
                    chat_groups.append(", ".join(current_group))

                me = await self.client.get_me()
                base_message = f"⚠️ Рассылка '{code_name}': Не удалось отправить сообщения в чаты:\n"

                for group in chat_groups:
                    message = base_message + group
                    try:
                        await self.client.send_message(
                            me.id,
                            message,
                            schedule=datetime.now() + timedelta(seconds=60),
                        )
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления: {e}")

        except Exception as e:
            logger.error(
                f"Ошибка обработки неудачных чатов для {code_name}: {e}"
            )

    @staticmethod
    def _chunk_messages(
        messages: List[Union[Message, List[Message]]], chunk_size: int = 10
    ) -> List[List[Union[Message, List[Message]]]]:
        """Разбивает список сообщений на части оптимального размера."""
        return [
            messages[i : i + chunk_size]
            for i in range(0, len(messages), chunk_size)
        ]

    async def _process_message_batch(
        self, code: Broadcast, messages: List[dict]
    ) -> Tuple[List[Union[Message, List[Message]]], List[dict]]:
        """Обрабатывает пакет сообщений с оптимизированной загрузкой."""
        messages_to_send = []
        deleted_messages = []

        results = await asyncio.gather(
            *[self._fetch_messages(msg) for msg in messages],
            return_exceptions=True,
        )

        for msg_data, result in zip(messages, results):
            if isinstance(result, Exception):
                logger.error(
                    f"Ошибка загрузки сообщения {msg_data['message_id']}: {result}"
                )
                deleted_messages.append(msg_data)
            elif result:
                messages_to_send.append(result)
            else:
                deleted_messages.append(msg_data)

        return messages_to_send, deleted_messages

    async def _broadcast_loop(self, code_name: str):
        """Main broadcast loop."""
        retry_count = 0
        max_retries = 3

        while self._active:
            try:
                code = self.codes.get(code_name)
                if not code or not code._active:
                    await asyncio.sleep(60)
                    continue

                if not code.chats or not code.messages:
                    await asyncio.sleep(60)
                    continue

                await self._apply_interval(code, code_name)

                messages_to_send = []
                deleted_messages = []
                batch_size = 20

                for i in range(0, len(code.messages), batch_size):
                    if not self._active or not code._active:
                        break

                    batch = code.messages[i : i + batch_size]
                    batch_messages, deleted = await self._process_message_batch(
                        code, batch
                    )
                    messages_to_send.extend(batch_messages)
                    deleted_messages.extend(deleted)

                if deleted_messages:
                    code.messages = [
                        m for m in code.messages if m not in deleted_messages
                    ]
                    await self.save_config()

                    if not code.messages:
                        logger.warning(
                            f"Все сообщения в коде {code_name} недоступны"
                        )
                        continue

                if not messages_to_send:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(
                            f"Достигнут лимит попыток получения сообщений для {code_name}"
                        )
                        await asyncio.sleep(300)
                        retry_count = 0
                    else:
                        await asyncio.sleep(60)
                    continue

                retry_count = 0

                if not code.batch_mode:
                    next_index = code.get_next_message_index()
                    messages_to_send = [
                        messages_to_send[next_index % len(messages_to_send)]
                    ]

                failed_chats = await self._send_messages_to_chats(
                    code, code_name, messages_to_send
                )

                if failed_chats:
                    await self._handle_failed_chats(code_name, failed_chats)

                current_time = time.time()
                self.last_broadcast_time[code_name] = current_time

                try:
                    async with self._lock:
                        saved_times = self.db.get(
                            "broadcast", "last_broadcast_times", {}
                        )
                        saved_times[code_name] = current_time
                        self.db.set(
                            "broadcast", "last_broadcast_times", saved_times
                        )
                except Exception as e:
                    logger.error(f"Ошибка сохранения времени рассылки: {e}")

            except asyncio.CancelledError:
                logger.info(f"Рассылка {code_name} остановлена")
                break
            except Exception as e:
                logger.error(
                    f"Критическая ошибка в цикле рассылки {code_name}: {e}"
                )
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(
                        f"Достигнут лимит попыток выполнения рассылки {code_name}"
                    )
                    await asyncio.sleep(300)
                    retry_count = 0
                else:
                    await asyncio.sleep(60)

    async def watcher(self, message: Message):
        """Автоматически добавляет чаты в рассылку."""
        try:
            if not self.watcher_enabled:
                return

            if not message or not message.text:
                return

            if not message.text.startswith("!"):
                return

            if message.sender_id != self.me_id:
                return

            parts = message.text.split()
            if len(parts) != 2:
                return

            code_name = parts[0][1:]
            chat_id = message.chat_id

            if code_name not in self.codes:
                return

            code = self.codes[code_name]

            if len(code.chats) >= self.MAX_CHATS_PER_CODE:
                return

            if chat_id not in code.chats:
                code.chats.add(chat_id)
                self.save_config()

        except Exception as e:
            logger.error(f"Error in watcher: {e}")

    async def on_unload(self):
        """Cleanup on module unload."""
        self._active = False

        if self._cleanup_task:
            self._cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cleanup_task

        if self._periodic_task:
            self._periodic_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._periodic_task

        for task in self.broadcast_tasks.values():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _fetch_messages(
        self, msg_data: dict
    ) -> Optional[Union[Message, List[Message]]]:
        """Получает сообщения с учетом ограничений размера медиа."""
        key = (msg_data["chat_id"], msg_data["message_id"])

        try:
            cached = await self._message_cache.get(key)
            if cached:
                return cached

            message_ids = msg_data.get("grouped_ids", [msg_data["message_id"]])

            messages = []
            for i in range(0, len(message_ids), 100):
                batch = message_ids[i : i + 100]
                batch_messages = await self.client.get_messages(
                    msg_data["chat_id"], ids=batch
                )
                messages.extend(m for m in batch_messages if m)

            if not messages:
                logger.warning(
                    f"Не удалось получить сообщение {msg_data['message_id']} из чата {msg_data['chat_id']}"
                )
                return None

            for msg in messages:
                if hasattr(msg, "media") and msg.media:
                    if hasattr(msg.media, "document") and hasattr(
                        msg.media.document, "size"
                    ):
                        if msg.media.document.size > 10 * 1024 * 1024:
                            logger.warning(
                                f"Медиа в сообщении {msg.id} превышает лимит размера (10MB)"
                            )
                            return None

            if len(message_ids) > 1:
                messages.sort(key=lambda x: message_ids.index(x.id))

            if messages:
                await self._message_cache.set(key, messages)
                return messages[0] if len(messages) == 1 else messages

            return None

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Ошибка сети при получении сообщений: {e}")
            return None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при получении сообщений: {e}")
            return None

    async def _get_chat_id(self, chat_identifier: str) -> Optional[int]:
        """Получает ID чата из разных форматов (ссылка, юзернейм, ID)"""
        try:
            if chat_identifier.lstrip("-").isdigit():
                return int(chat_identifier)

            clean_username = chat_identifier.lower()
            for prefix in ["https://", "http://", "t.me/", "@", "telegram.me/"]:
                clean_username = clean_username.replace(prefix, "")

            entity = await self.client.get_entity(clean_username)
            return entity.id

        except Exception as e:
            logger.error(f"Ошибка получения chat_id: {e}")
            return None
