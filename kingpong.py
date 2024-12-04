from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Union, Optional, Callable

from telethon import TelegramClient
from telethon.tl.types import (
    Chat, 
    Channel, 
    User, 
    Message,
    ChannelParticipantAdmin,
    ChannelParticipantCreator
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.errors import (
    ChatAdminRequiredError, 
    FloodWaitError, 
    RPCError, 
    ChatForbiddenError,
    UserNotParticipantError
)
from telethon.tl.types import InputPeerChannel, InputPeerChat

from .. import loader, utils

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class PerformanceProfiler:
    """Профессиональный замер производительности"""
    
    @staticmethod
    def measure_time(threshold_ms: float = 500) -> Callable:
        """Декоратор точного измерения времени выполнения с настраиваемым порогом"""
        def decorator(func):
            async def wrapper(*args, **kwargs):
                start_time = asyncio.get_event_loop().time()
                try:
                    return await func(*args, **kwargs)
                finally:
                    execution_time = (asyncio.get_event_loop().time() - start_time) * 1000
                    if execution_time > threshold_ms:
                        logging.warning(
                            f"Slow operation: {func.__name__} "
                            f"took {execution_time:.2f} ms"
                        )
            return wrapper
        return decorator

@dataclass(frozen=True)
class ChatStatistics:
    """Неизменяемый контейнер статистики чата с эффективным хранением"""
    title: str = 'Неизвестно'
    chat_id: int = 0
    chat_type: str = 'Неизвестно'
    total_members: int = 0
    active_members: int = 0
    admins: int = 0
    bots: int = 0
    total_messages: int = 0
    
    def format(self, ping: float) -> str:
        """Безопасное форматирование статистики с HTML-экранированием"""
        return (
            f"🏓 <b>Пинг:</b> {ping:.2f} мс\n\n"
            f"📊 <b>{utils.escape_html(self.title)}:</b>\n"
            f"ID: <code>{self.chat_id}</code>\n"
            f"Тип: {self.chat_type}\n"
            f"Участники: {self.total_members}\n"
            f"Активные: {self.active_members}\n"
            f"Администраторы: {self.admins}\n"
            f"Боты: {self.bots}\n"
            f"Сообщений: {self.total_messages}"
        )

@loader.tds
class PingKongModule(loader.Module):
    """Профессиональный модуль статистики чата"""

    strings = {
        "name": "EnhancedPing", 
        "error": "❌ <b>Ошибка:</b> {}"
    }

    def __init__(self):
        self._client: Optional[TelegramClient] = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def client_ready(self, client, db):
        """Безопасная инициализация клиента"""
        self._client = client
        self._logger.info("EnhancedPing module initialized")

    @PerformanceProfiler.measure_time()
    async def _get_precise_ping(self) -> float:
        """Точное измерение задержки с расширенной обработкой ошибок"""
        try:
            start = asyncio.get_event_loop().time()
            await self._client.get_me()
            return (asyncio.get_event_loop().time() - start) * 1000
        except Exception as e:
            self._logger.error(f"Ping measurement error: {e}")
            return -1.0

    @PerformanceProfiler.measure_time()
    async def _count_user_messages(
        self, 
        chat: Union[Chat, Channel, InputPeerChannel, InputPeerChat], 
        limit: int = 5000
    ) -> int:
        """Оптимизированный подсчет сообщений с обработкой ограничений"""
        try:
            messages = await self._client.get_messages(chat, limit=limit)
            return len(messages)
        except (FloodWaitError, RPCError) as e:
            self._logger.warning(f"Message counting limited: {e}")
            return 0

    @PerformanceProfiler.measure_time()
    async def _get_admin_count(
        self, 
        chat: Union[Chat, Channel, InputPeerChannel, InputPeerChat]
    ) -> int:
        """Многоуровневое определение количества администраторов"""
        try:
            # Попытка получить администраторов напрямую
            participants = await self._client.get_participants(
                chat, 
                filter=lambda p: isinstance(p, (ChannelParticipantAdmin, ChannelParticipantCreator))
            )
            return len(participants)
        except (UserNotParticipantError, ChatAdminRequiredError):
            # Резервный метод получения информации о чате
            try:
                if isinstance(chat, Channel):
                    full_chat = await self._client(GetFullChannelRequest(chat))
                    return getattr(full_chat.full_chat, 'admin_count', 0)
                elif isinstance(chat, Chat):
                    full_chat = await self._client(GetFullChatRequest(chat.id))
                    return getattr(full_chat.full_chat, 'admin_count', 0)
            except Exception as inner_e:
                self._logger.error(f"Detailed admin count error: {inner_e}")
                return 0
        except Exception as e:
            self._logger.error(f"Admin counting error: {e}")
            return 0

    @PerformanceProfiler.measure_time()
    async def _analyze_chat_comprehensive(
        self, 
        chat: Union[Chat, Channel, InputPeerChannel, InputPeerChat]
    ) -> ChatStatistics:
        """Комплексный анализ чата с расширенной диагностикой"""
        try:
            # Определение типа чата с использованием match-like подхода
            chat_type = (
                "Супер-группа" if getattr(chat, 'megagroup', False) 
                else "Канал" if isinstance(chat, Channel) 
                else "Группа" if isinstance(chat, Chat) 
                else "Неизвестный тип"
            )

            chat_id = getattr(chat, 'id', 0)
            title = getattr(chat, 'title', 'Неизвестно')

            # Безопасное получение количества участников
            try:
                if isinstance(chat, Channel):
                    full_chat = await self._client(GetFullChannelRequest(chat))
                    total_members = getattr(full_chat.full_chat, 'participants_count', 0)
                elif isinstance(chat, Chat):
                    full_chat = await self._client(GetFullChatRequest(chat.id))
                    total_members = getattr(full_chat.full_chat, 'participants_count', 0)
                else:
                    total_members = 0
            except Exception as e:
                self._logger.warning(f"Total members counting error: {e}")
                total_members = 0

            # Подсчет администраторов
            admins = await self._get_admin_count(chat)

            # Расширенный подсчет участников
            try:
                all_participants = await self._client.get_participants(chat, aggressive=True)
                total_participants = len(all_participants)
                active_members = sum(
                    1 for p in all_participants 
                    if not p.deleted and 
                       not p.bot and 
                       not p.is_self and 
                       p.access_hash is not None
                )
                bots = sum(1 for p in all_participants if p.bot)
            except Exception as e:
                self._logger.warning(f"Participants count error: {e}")
                total_participants = total_members
                active_members = 0
                bots = 0

            # Подсчет сообщений
            total_messages = await self._count_user_messages(chat)

            return ChatStatistics(
                title=title,
                chat_id=chat_id,
                chat_type=chat_type,
                total_members=total_members,
                active_members=active_members,
                admins=admins,
                bots=bots,
                total_messages=total_messages
            )
        except Exception as e:
            self._logger.error(f"Comprehensive chat analysis failed: {e}")
            return ChatStatistics(chat_id=chat_id)

    @loader.command()
    async def pong(self, message):
        """Команда получения статистики чата"""
        try:
            ping_time = await self._get_precise_ping()
            chat = await self._client.get_entity(message.chat_id)
            chat_stats = await self._analyze_chat_comprehensive(chat)

            async def refresh_callback(call):
                new_ping = await self._get_precise_ping()
                await call.edit(
                    chat_stats.format(new_ping),
                    reply_markup=self._create_refresh_markup(refresh_callback)
                )

            await self.inline.form(
                chat_stats.format(ping_time),
                message=message,
                reply_markup=self._create_refresh_markup(refresh_callback)
            )
        except Exception as e:
            await self.inline.form(
                self.strings["error"].format(str(e)), 
                message=message
            )

    def _create_refresh_markup(self, callback: Optional[Callable] = None):
        """Создание унифицированной разметки обновления"""
        return [{"text": "🔄 Обновить", "callback": callback or (lambda _: None)}]
