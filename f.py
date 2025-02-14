"""Author: kramiikk - Telegram: @kramiikk"""

import asyncio
import logging
import random
import time
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from hikkatl.tl.types import Message
from hikkatl.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    UserBannedInChannelError,
)

from .. import loader, utils
from ..tl_cache import CustomTelegramClient

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, max_requests: int, time_window: int):
        self._lock = asyncio.Lock()
        self.max_requests = max_requests
        self.time_window = time_window
        self.timestamps = deque(maxlen=max_requests * 2)

    async def acquire(self):
        async with self._lock:
            current_time = time.monotonic()

            while (
                self.timestamps
                and self.timestamps[0] <= current_time - self.time_window
            ):
                self.timestamps.popleft()
            if len(self.timestamps) >= self.max_requests:
                wait_time = self.timestamps[0] + self.time_window - current_time
                await asyncio.sleep(wait_time)
                current_time = time.monotonic()
                while (
                    self.timestamps
                    and self.timestamps[0] <= current_time - self.time_window
                ):
                    self.timestamps.popleft()
            self.timestamps.append(current_time)


class SimpleCache:
    def __init__(self, ttl: int = 7200, max_size: int = 20):
        self._active = True
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self._lock = asyncio.Lock()

    async def clean_expired(self, force: bool = False):
        async with self._lock:
            if not force and len(self.cache) < self.max_size // 2:
                return
            current_time = time.time()
            expired = [
                k
                for k, (expire_time, _) in self.cache.items()
                if current_time > expire_time
            ]
            for key in expired:
                del self.cache[key]

    async def get(self, key: tuple):
        """Get a value from cache using a tuple key"""
        async with self._lock:
            entry = self.cache.get(key)
            if not entry:
                return None
            expire_time, value = entry
            if time.time() > expire_time:
                del self.cache[key]
                return None
            self.cache.move_to_end(key)
            return value

    async def set(self, key: tuple, value, expire: Optional[int] = None):
        """Set a value in cache using a tuple key"""
        async with self._lock:
            if expire is not None and expire <= 0:
                return
            ttl = expire if expire is not None else self.ttl
            expire_time = time.time() + ttl
            if key in self.cache:
                del self.cache[key]
            self.cache[key] = (expire_time, value)
            while len(self.cache) > self.max_size:
                self.cache.popitem(last=False)

    async def start_auto_cleanup(self):
        while self._active:
            await self.clean_expired()
            try:
                await asyncio.sleep(self.ttl)
            except asyncio.CancelledError:
                break


class BroadcastMod(loader.Module):
    """Модуль для массовой рассылки."""

    def __init__(self):
        self.manager = None

    @loader.command()
    async def b(self, message):
        """Команда для управления рассылкой."""
        await self.manager.handle_command(message)

    async def client_ready(self):
        """Инициализация модуля при загрузке"""
        self.manager = BroadcastManager(self.client, self.db, self.tg_id)
        await self.manager.load_config()

        self.manager.adaptive_interval_task = asyncio.create_task(
            self.manager.start_adaptive_interval_adjustment()
        )
        self.manager.cache_cleanup_task = asyncio.create_task(
            self.manager._message_cache.start_auto_cleanup()
        )

        for code_name, code in self.manager.codes.items():
            if code._active and code.messages and code.chats:
                self.manager.broadcast_tasks[code_name] = asyncio.create_task(
                    self.manager._broadcast_loop(code_name)
                )

    async def on_unload(self):
        if not hasattr(self, "manager"):
            return
        self.manager._active = False

        tasks = []
        tasks.extend(self.manager.broadcast_tasks.values())

        if self.manager.adaptive_interval_task:
            tasks.append(self.manager.adaptive_interval_task)
        if self.manager.cache_cleanup_task:
            tasks.append(self.manager.cache_cleanup_task)
        for task in tasks:
            if task and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if hasattr(self.manager, "_message_cache"):
            await self.manager._message_cache.clean_expired(force=True)

    async def watcher(self, message):
        """Автоматически отвечает на первое сообщение."""
        if not self.manager.watcher_enabled or not isinstance(message, Message) or not message.out or not message.text:
            return
        if message.text.startswith("💫"):
            parts = message.text.split()
            code_name = parts[0][1:]
            if code_name.isalnum():
                chat_id = message.chat_id
                code = self.manager.codes.get(code_name)
                if code and len(code.chats) < 250 and chat_id not in code.chats:
                    code.chats.add(chat_id)
                    await self.manager.save_config()


@dataclass
class Broadcast:
    chats: Set[int] = field(default_factory=set)
    messages: Set[Tuple[int, int]] = field(default_factory=set)
    interval: Tuple[int, int] = (5, 6)
    _active: bool = field(default=False, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    original_interval: Tuple[int, int] = (5, 6)
    groups: List[List[int]] = field(default_factory=list)
    last_group_chats: Set[int] = field(default_factory=set)


class BroadcastManager:
    """Manages broadcast operations and state."""

    GLOBAL_LIMITER = RateLimiter(max_requests=20, time_window=60)

    def __init__(self, client: CustomTelegramClient, db, tg_id):
        self.client = client
        self.db = db
        self.codes: Dict[str, Broadcast] = {}
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self._message_cache = SimpleCache(ttl=7200, max_size=20)
        self._active = True
        self._lock = asyncio.Lock()
        self.watcher_enabled = False
        self.cache_cleanup_task = None
        self.tg_id = tg_id
        self.pause_event = asyncio.Event()
        self.pause_event.clear()
        self.last_flood_time = 0
        self.flood_wait_times = []
        self.chat_last_sent = {}
        self.group_last_sent = {}
        self.adaptive_multiplier = 1.0
        self.adaptive_interval_task = None

    async def _broadcast_loop(self, code_name: str):
        code = self.codes.get(code_name)
        if not code or not code.messages or not code.chats:
            return
        while self._active and code._active and not self.pause_event.is_set():
            async with code._lock:
                if not code.messages or not code.chats:
                    return
                try:
                    code.interval = self._calculate_safe_interval(len(code.chats))

                    await asyncio.sleep(
                        random.uniform(code.interval[0], code.interval[1]) * 60
                    )

                    loop_start = time.monotonic()

                    if code.last_group_chats != code.chats:
                        chats = list(code.chats)
                        random.shuffle(chats)
                        code.groups = [
                            chats[i : i + 20] for i in range(0, len(chats), 20)
                        ]
                        code.last_group_chats = set(code.chats)
                        self.group_last_sent.clear()
                    groups = code.groups
                    if not groups:
                        await asyncio.sleep(2)
                        continue
                    msg_tuple = random.choice(tuple(code.messages))
                    message = await self._fetch_message(*msg_tuple)
                    if not message:
                        code.messages.remove(msg_tuple)
                        await self.save_config()
                        continue
                    for group_idx, group in enumerate(code.groups):
                        last_group = self.group_last_sent.get(group_idx, 0)
                        if (
                            wait := (
                                code.interval[0] * 60 - (time.monotonic() - last_group)
                            )
                        ) > 0:
                            await asyncio.sleep(wait * self.adaptive_multiplier)
                        for chat_id in group:
                            last_chat = self.chat_last_sent.get(chat_id, 0)
                            if (
                                wait := (
                                    code.interval[0] * 60
                                    - (time.monotonic() - last_chat)
                                )
                            ) > 0:
                                await asyncio.sleep(wait * self.adaptive_multiplier)
                            await self.GLOBAL_LIMITER.acquire()
                            if await self._send_message(chat_id, message):
                                self.chat_last_sent[chat_id] = time.monotonic()
                            await asyncio.sleep(3)
                        self.group_last_sent[group_idx] = time.monotonic()
                    elapsed = time.monotonic() - loop_start
                    if elapsed > code.interval[1] * 60:
                        self.adaptive_multiplier *= 0.9
                    else:
                        self.adaptive_multiplier *= 1.1
                    self.adaptive_multiplier = max(1, min(2, self.adaptive_multiplier))
                except asyncio.CancelledError:
                    logger.info(f"Broadcast {code_name} cancelled")
                    raise
                except Exception as e:
                    logger.error(f"[{code_name}] Critical error: {e}")

    def _calculate_safe_interval(self, total_chats: int) -> Tuple[int, int]:
        if total_chats <= 2:
            safe_min = 2
        elif total_chats >= 250:
            safe_min = 10
        else:
            safe_min = 2 + (total_chats - 2) * 8 / 248
            safe_min = int(round(safe_min))
        variance = max(2, int(safe_min * 0.2))
        safe_max = safe_min + variance
        safe_max = min(safe_max, 1440)
        return (safe_min, safe_max)

    async def _check_and_adjust_intervals(self):
        """Проверка условий для восстановления интервалов"""
        async with self._lock:
            if not self.flood_wait_times:
                return
            if (time.time() - self.last_flood_time) > 43200:
                for code in self.codes.values():
                    code.interval = code.original_interval
                self.flood_wait_times = []
                await self.client.dispatcher.safe_api_call(
                    await self.client.send_message(
                        self.tg_id,
                        "🔄 12 часов без ошибок! Интервалы восстановлены до исходных",
                    )
                )
            else:
                for code_name, code in self.codes.items():
                    new_min = max(2, int(code.interval[0] * 0.85))
                    new_max = max(min(int(code.interval[1] * 0.85), 1440), new_min + 2)
                    code.interval = (new_min, new_max)
                    await self.client.dispatcher.safe_api_call(
                        await self.client.send_message(
                            self.tg_id,
                            f"⏱ Автокоррекция интервалов для {code_name}: {new_min}-{new_max} минут",
                        )
                    )
            await self.save_config()

    async def _fetch_message(self, chat_id: int, message_id: int):
        """Fetch a message from cache or Telegram"""
        cache_key = (chat_id, message_id)

        cached = await self._message_cache.get(cache_key)
        if cached:
            return cached
        try:
            msg = await self.client.get_messages(entity=chat_id, ids=message_id)
            if msg:
                await self._message_cache.set(cache_key, msg)
                return msg
            logger.error(f"Сообщение {chat_id}:{message_id} не найдено")
            return None
        except ValueError as e:
            logger.error(f"Чат/сообщение не существует: {chat_id} {message_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении сообщения: {e}", exc_info=True)
            return None

    async def _generate_stats_report(self) -> str:
        """Генерация отчета: .br l"""
        if not self.codes:
            return "😶‍🌫️ Нет активных рассылок"
        report = ["🎩 <strong>Статистика рассылок</strong>"]
        for code_name, code in self.codes.items():
            report.append(
                f"\n▸ <code>{code_name}</code> {'✨' if code._active else '🧊'}\n"
                f"├ Чатов: {len(code.chats)}\n"
                f"├ Интервал: {code.interval[0]}-{code.interval[1]} мин\n"
                f"└ Сообщений: {len(code.messages)}\n"
            )
        return "".join(report)

    async def _handle_add(self, message, code, code_name, args) -> str:
        """Добавление сообщения в рассылку: .br a [code]"""
        reply = await message.get_reply_message()
        if not reply:
            return "🫵 Ответьте на сообщение"
        if not code:
            code = Broadcast()
            self.codes[code_name] = code
        key = (reply.chat_id, reply.id)
        if key in code.messages:
            return "ℹ️ Сообщение уже добавлено"
        code.messages.add(key)
        await self._message_cache.set(key, reply)
        await self.save_config()

        return f"🍑 <code>{code_name}</code> | Сообщений: {len(code.messages)}"

    async def _handle_add_chat(self, message, code, code_name, args) -> str:
        """Добавление чата: .br ac [code] [@chat]"""
        target = args[2] if len(args) > 2 else message.chat_id
        chat_id = await self._parse_chat_identifier(target)

        if not chat_id:
            return "🫵 Неверный формат чата"
        if chat_id in code.chats:
            return "ℹ️ Чат уже добавлен"
        if len(code.chats) >= 250:
            return "🫵 Лимит 250 чатов"
        code.chats.add(chat_id)
        await self.save_config()
        return f"🪴 +1 чат | Всего: {len(code.chats)}"

    async def _handle_delete(self, message, code, code_name, args) -> str:
        """Удаление рассылки: .br d [code]"""
        if code_name in self.broadcast_tasks:
            self.broadcast_tasks[code_name].cancel()
        del self.codes[code_name]
        await self.save_config()
        return f"🗑 {code_name} удалена"

    async def _handle_interval(self, message, code, code_name, args) -> str:
        if len(args) < 4:
            return "Укажите мин/макс"
        try:
            requested_min = int(args[2])
            requested_max = int(args[3])
            if requested_min >= requested_max:
                return "🛑 Минимум должен быть меньше максимума"
        except ValueError:
            return "Некорректные значения"
        safe_min, safe_max = self._calculate_safe_interval(len(code.chats))
        if requested_min < safe_min:
            return f"⚠️ Указанные значения недопустимы. Безопасный интервал {safe_min}-{safe_max}."
        code.interval = (requested_min, requested_max)
        code.original_interval = code.interval
        await self.save_config()
        return f"⏱️ Интервал для {code_name}: {requested_min}-{requested_max} мин"

    async def _handle_flood_wait(self, e: FloodWaitError, chat_id: int):
        """Глобальная обработка FloodWait с остановкой всех рассылок"""
        async with self._lock:
            if self.pause_event.is_set():
                return False
            self.pause_event.set()
            avg_wait = (
                sum(self.flood_wait_times[-3:]) / len(self.flood_wait_times[-3:])
                if self.flood_wait_times
                else 0
            )
            wait_time = min(max(e.seconds + 15, avg_wait * 1.5), 7200)

            self.last_flood_time = time.time()
            self.flood_wait_times.append(wait_time)
            if len(self.flood_wait_times) > 10:
                self.flood_wait_times = self.flood_wait_times[-10:]
            await self.client.dispatcher.safe_api_call(
                await self.client.send_message(
                    self.tg_id,
                    f"🚨 Обнаружен FloodWait {e.seconds}s! Все рассылки приостановлены на {wait_time}s",
                )
            )
            logger.error(
                f"🚨 FloodWait {e.seconds} сек. в чате {chat_id}. Среднее время ожидания: {avg_wait:.1f} сек. "
                f"Всего FloodWait за последние 12 часов: {len(self.flood_wait_times)}"
            )

            tasks = list(self.broadcast_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(wait_time)

            self.pause_event.clear()
            await self._restart_all_broadcasts()
            await self.client.dispatcher.safe_api_call(
                await self.client.send_message(
                    self.tg_id,
                    "🐈 Глобальная пауза снята. Рассылки возобновлены",
                )
            )

            for code in self.codes.values():
                code.interval = (
                    min(code.interval[0] * 2, 120),
                    min(code.interval[1] * 2, 240),
                )
                if not hasattr(code, "original_interval"):
                    code.original_interval = code.interval
            await self.save_config()

    async def _handle_permanent_error(self, chat_id: int):
        async with self._lock:
            modified = False
            for code in self.codes.values():
                if chat_id in code.chats:
                    code.chats.discard(chat_id)
                    modified = True
            if modified:
                await self.save_config()

    async def _handle_remove(self, message, code, code_name, args) -> str:
        """Удаление сообщения: .br r [code]"""
        reply = await message.get_reply_message()
        if not reply:
            return "🫵 Ответьте на сообщение"
        key = (reply.chat_id, reply.id)
        if key not in code.messages:
            return "🫵 Сообщение не найдено"
        code.messages.remove(key)
        await self._message_cache.set(key, None)
        await self.save_config()
        return f"🐀 Удалено | Осталось: {len(code.messages)}"

    async def _handle_remove_chat(self, message, code, code_name, args) -> str:
        """Удаление чата: .br rc [code] [@chat]"""
        target = args[2] if len(args) > 2 else message.chat_id
        chat_id = await self._parse_chat_identifier(target)

        if not chat_id:
            return "🫵 Неверный формат чата"
        if chat_id not in code.chats:
            return "ℹ️ Чат не найден"
        code.chats.remove(chat_id)
        await self.save_config()
        return f"🐲 -1 чат | Осталось: {len(code.chats)}"

    async def _handle_start(self, message, code, code_name, args) -> str:
        """Запуск рассылки: .br s [code]"""
        if not code.messages:
            return "🫵 Нет сообщений для отправки"
        if not code.chats:
            return "🫵 Нет чатов для рассылки"
        if code._active:
            return "ℹ️ Рассылка уже активна"
        code._active = True
        self.broadcast_tasks[code_name] = asyncio.create_task(
            self._broadcast_loop(code_name)
        )

        await self.save_config()

        return f"🚀 {code_name} запущена | Чатов: {len(code.chats)}"

    async def _handle_stop(self, message, code, code_name, args) -> str:
        """Остановка рассылки: .br x [code]"""
        if not code._active:
            return "ℹ️ Рассылка не активна"
        code._active = False
        if code_name in self.broadcast_tasks:
            self.broadcast_tasks[code_name].cancel()
        await self.save_config()

        return f"🧊 {code_name} остановлена"

    async def _parse_chat_identifier(self, identifier) -> Optional[int]:
        """Парсинг идентификатора чата"""
        try:
            if identifier is None:
                return None
            if isinstance(identifier, int) or str(identifier).lstrip("-").isdigit():
                return int(identifier)
            entity = await self.client.get_entity(identifier)
            return entity.id
        except Exception:
            return None

    async def _restart_all_broadcasts(self):
        async with self._lock:
            for code_name, code in self.codes.items():
                if code._active:
                    if task := self.broadcast_tasks.get(code_name):
                        if not task.done() and not task.cancelled():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )
                    logger.info(f"Перезапуск рассылки: {code_name}")

    async def _send_message(self, chat_id: int, msg) -> bool:
        if not self.pause_event.is_set():
            try:
                await self.GLOBAL_LIMITER.acquire()
                await self.client.forward_messages(
                    entity=chat_id, messages=msg.id, from_peer=msg.chat_id
                )
                return True
            except FloodWaitError as e:
                await self._handle_flood_wait(e, chat_id)
            except (ChatWriteForbiddenError, UserBannedInChannelError):
                await self._handle_permanent_error(chat_id)
            except Exception as e:
                logger.error(f"In {chat_id}: {repr(e)}")
        return False

    def _toggle_watcher(self, args) -> str:
        """Переключение авто-добавления: .br w [on/off]"""
        if len(args) < 2:
            return f"🔍 Автодобавление: {'ON' if self.watcher_enabled else 'OFF'}"
        self.watcher_enabled = args[1].lower() == "on"
        return f"🐺 Автодобавление: {'ВКЛ' if self.watcher_enabled else 'ВЫКЛ'}"

    async def _validate_loaded_data(self):
        """Базовая проверка загруженных данных"""
        for code_name, code in self.codes.items():
            if code._active and (not code.messages or not code.chats):
                logger.info(f"Отключение {code_name}: нет сообщений/чатов")
                code._active = False
            if not (0 < code.interval[0] < code.interval[1] <= 1440):
                logger.info(f"Сброс интервала для {code_name}")
                code.interval = (5, 6)
                code.original_interval = (5, 6)

    async def handle_command(self, message):
        """Обработчик команд управления рассылкой"""
        response = None
        args = message.text.split()[1:]

        if not args:
            response = "🫵 Недостаточно аргументов"
        else:
            action = args[0].lower()

            if action == "l":
                response = await self._generate_stats_report()
            elif action == "w":
                response = self._toggle_watcher(args)
            else:
                code_name = args[1] if len(args) > 1 else None
                if not code_name:
                    response = "🫵 Укажите код рассылки"
                else:
                    code = self.codes.get(code_name)
                    handler_map = {
                        "a": self._handle_add,
                        "d": self._handle_delete,
                        "r": self._handle_remove,
                        "ac": self._handle_add_chat,
                        "rc": self._handle_remove_chat,
                        "i": self._handle_interval,
                        "s": self._handle_start,
                        "x": self._handle_stop,
                    }

                    if action not in handler_map:
                        response = "🫵 Неизвестная команда"
                    elif action != "a" and not code:
                        response = f"🫵 Рассылка {code_name} не найдена"
                    else:
                        try:
                            handler = handler_map[action]
                            result = await handler(message, code, code_name, args)
                            response = result
                        except Exception as e:
                            logger.error(f"Command error: {e}")
                            response = f"🚨 Ошибка: {str(e)}"
        await utils.answer(message, response)

    async def load_config(self):
        """Загрузка конфигурации с базовой валидацией"""
        try:
            raw_config = self.db.get("broadcast", "config") or {}

            for code_name, code_data in raw_config.get("codes", {}).items():
                try:
                    code = Broadcast(
                        chats=set(map(int, code_data.get("chats", []))),
                        messages={
                            (int(msg["chat_id"]), int(msg["message_id"]))
                            for msg in code_data.get("messages", [])
                        },
                        interval=tuple(map(int, code_data.get("interval", (5, 6)))),
                        original_interval=tuple(
                            map(int, code_data.get("original_interval", (5, 6)))
                        ),
                    )

                    code.groups = [
                        [int(chat) for chat in group if int(chat) in code.chats]
                        for group in code_data.get("groups", [])
                    ]
                    code.last_group_chats = set(
                        map(int, code_data.get("last_group_chats", []))
                    )

                    code._active = code_data.get("active", False)

                    self.codes[code_name] = code
                except Exception as e:
                    logger.error(f"Ошибка загрузки {code_name}: {str(e)}")
                    continue
            await self._validate_loaded_data()
        except Exception as e:
            logger.error(f"Критическая ошибка загрузки: {str(e)}", exc_info=True)
            self.codes = {}

    async def save_config(self):
        """Сохранение конфигурации"""
        try:
            async with self._lock:
                config = {
                    "codes": {
                        name: {
                            "chats": list(code.chats),
                            "messages": [
                                {"chat_id": cid, "message_id": mid}
                                for cid, mid in code.messages
                            ],
                            "interval": list(code.interval),
                            "original_interval": list(code.original_interval),
                            "active": code._active,
                            "groups": code.groups,
                            "last_group_chats": list(code.last_group_chats),
                        }
                        for name, code in self.codes.items()
                    }
                }
                try:
                    self.db.set("broadcast", "config", config)
                except Exception as e:
                    logger.error(f"Database error during save: {e}")
                    raise
        except Exception as e:
            logger.error(f"Critical error during save: {e}")
            raise

    async def start_adaptive_interval_adjustment(self):
        """Фоновая задача для адаптации интервалов"""
        while self._active:
            try:
                await asyncio.sleep(1800)
                await self._check_and_adjust_intervals()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в адаптивной регулировке: {e}", exc_info=True)
