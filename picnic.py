""" Author: kramiikk - Telegram: @ilvij """

import asyncio
import logging
import random
from datetime import datetime
from collections import deque
from typing import Optional, Dict, Union, List
from telethon import functions, types, errors
from telethon.errors.rpcerrorlist import (
    MessageIdInvalidError,
    PhotoInvalidDimensionsError,
    PhotoCropSizeSmallError,
    PhotoSaveFileInvalidError,
)
from .. import loader, utils
import json
import os
import re

logger = logging.getLogger(__name__)

CONFIG_ADAPTIVE_DELAY = "adaptive_delay"
CONFIG_NOTIFY_ERRORS = "notify_errors"
CONFIG_DEFAULT_DELAY = "default_delay"
CONFIG_MIN_DELAY = "min_delay"
CONFIG_MAX_DELAY = "max_delay"
CONFIG_JITTER = "jitter"
CONFIG_ERROR_THRESHOLD = "error_threshold"
CONFIG_SUCCESS_REDUCTION = "success_reduction"
CONFIG_DELAY_MULTIPLIER = "delay_multiplier"


@loader.tds
class ProfileChangerMod(loader.Module):
    """Автоматическое обновление фото профиля с адаптивной системой защиты."""

    strings = {
        "name": "ProfileChanger",
        "starting": "🔄 <b>Запуск смены фото профиля</b>\n\n• Задержка: {delay_minutes:.0f} мин\n• ~{updates_per_hour} обновлений/час",
        "stopping": "🛑 <b>Остановка</b>\n• Обновлений: {count}\n• С момента запуска: {uptime}\n• Ошибок: {errors}",
        "stats": "📊 <b>Статистика</b>\n• Статус: {status}\n• С момента запуска: {uptime}\n• Обновлений: {count}\n• В час: {hourly}\n• Задержка: {delay:.1f} мин\n• Последнее: {last}\n• Ошибок: {errors}\n• Флудвейтов: {floods}\n\n⚙️ <b>Адаптация задержки:</b>\n{delay_details}",
        "no_photo": "❌ <b>Ответьте на фото</b>",
        "already_running": "⚠️ <b>Уже запущено</b>",
        "not_running": "⚠️ <b>Не запущено</b>",
        "error": "❌ <b>Ошибка:</b> {error}",
        "flood_wait": "⚠️ <b>Флудвейт</b>\n\n• Новая задержка: {delay:.1f} мин\n• Ожидание: {wait:.1f} мин",
        "photo_invalid": "⚠️ <b>Неверный формат фото:</b> {error}",
        "pfpone_success": "✅ <b>Аватарка установлена</b>",
        "pfpone_no_reply": "❌ <b>Ответьте на фото, которое хотите установить</b>",
        "pfpone_error": "❌ <b>Ошибка при установке аватарки:</b> {error}",
        "delay_details_success": "  • Успешные обновления: снижение задержки",
        "delay_details_recent_flood": "  • Недавний флудвейт: увеличение задержки",
        "delay_details_recent_error": "  • Недавняя ошибка: увеличение задержки",
        "delay_details_jitter": "  • Случайность: +/- {jitter_percent:.0f}%",
        "stopping_timeout": "⏳ <b>Остановка выполняется в фоновом режиме...</b>",
        "stopped_successfully": "✅ <b>Успешно остановлено</b>",
        "dir_not_found": "❌ <b>Директория не найдена: /root/Heroku/new</b>",
        "no_photos": "❌ <b>В директории нет подходящих фотографий</b>",
        "invalid_delay": "❌ <b>Неверная задержка. Используйте число секунд</b>",
        "loading_from_dir": "🔄 <b>Загрузка фотографий из директории...</b>\n\n• Найдено фото: {count}\n• Задержка: {delay} сек",
        "dir_complete": "✅ <b>Загрузка завершена</b>\n\n• Загружено: {uploaded}\n• Ошибок: {errors}\n• Прошло времени: {elapsed}",
        "current_photo": "📸 <b>Загружаю фото:</b> {photo}\n<b>Прогресс:</b> {current}/{total}",
    }

    _state_keys = [
        "running",
        "start_time",
        "last_update",
        "update_count",
        "error_count",
        "flood_count",
        "delay",
        "chat_id",
        "message_id",
        "success_streak",
        "floods",
        "retries",
        "last_error_time",
    ]

    def _init_state(self):
        """Инициализация состояния модуля"""
        self.running = False
        self._task = None
        self.start_time = None
        self.last_update = None
        self.update_count = 0
        self.error_count = 0
        self.flood_count = 0
        self.delay = self.config[CONFIG_DEFAULT_DELAY]
        self.chat_id = None
        self.message_id = None
        self.floods = deque(maxlen=10)
        self.success_streak = 0
        self.retries = 0
        self.last_error_time = None

    def __init__(self):
        self.config = loader.ModuleConfig(
            "adaptive_delay",
            True,
            "Адаптивные задержки",
            "notify_errors",
            True,
            "Уведомления об ошибках",
            "default_delay",
            813,
            "Начальная задержка (сек)",
            "min_delay",
            701,
            "Минимальная задержка (сек)",
            "max_delay",
            930,
            "Максимальная задержка (сек)",
            "jitter",
            0.3,
            "Случайность (0.0-1.0)",
            "error_threshold",
            3,
            "Порог ошибок",
            "success_reduction",
            0.9,
            "Снижение при успехе",
            "delay_multiplier",
            1.3,
            "Множитель задержки при ошибках и флудвейтах",
        )
        self.multiplier_ranges = [
            (0.85, 1.0),
            (1.15, 1.3),
            (1.45, 1.7),
            (0.85, 0.95),
            (1.15, 1.25),
            (1.45, 1.55),
        ]
        self.recent_multipliers = deque(maxlen=3)
        self.total_updates_cycle = 0
        self._lock = asyncio.Lock()
        self._init_state()

    def _reset(self) -> None:
        """Сброс состояния модуля, включая остановку текущей задачи."""
        self._init_state()
        self._task = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()
        self._load_state()
        if self.running:
            self._task = asyncio.create_task(self._loop())
        logger.info("ProfileChanger loaded")

    async def on_unload(self):
        """Выгрузка модуля."""
        if self.running:
            await self._send_stopping_message()
            if self._task:
                self._task.cancel()
        logger.info("ProfileChanger unloaded")

    def _get_state(self) -> Dict:
        """Получение текущего состояния модуля для сохранения."""
        floods = (
            list(self.floods) if hasattr(self, "floods") and self.floods else []
        )

        state = {
            "running": self.running,
            "start_time": (
                self.start_time.isoformat() if self.start_time else None
            ),
            "last_update": (
                self.last_update.isoformat() if self.last_update else None
            ),
            "update_count": self.update_count,
            "error_count": self.error_count,
            "flood_count": self.flood_count,
            "delay": self.delay,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "success_streak": self.success_streak,
            "floods": [t.isoformat() for t in floods],
            "retries": self.retries,
            "last_error_time": (
                self.last_error_time.isoformat()
                if self.last_error_time
                else None
            ),
        }
        return state

    def _load_state(self) -> None:
        """Загрузка состояния модуля из базы данных."""
        try:
            state_json = self._db.get(self.strings["name"], "state")
            if not state_json:
                return
            state = json.loads(state_json)

            if state.get("start_time"):
                state["start_time"] = datetime.fromisoformat(
                    state["start_time"]
                )
            if state.get("last_update"):
                state["last_update"] = datetime.fromisoformat(
                    state["last_update"]
                )
            if state.get("last_error_time"):
                state["last_error_time"] = datetime.fromisoformat(
                    state["last_error_time"]
                )
            if "floods" in state:
                floods_list = [
                    datetime.fromisoformat(t) for t in state["floods"]
                ]
                state["floods"] = deque(floods_list, maxlen=10)
            for key, value in state.items():
                setattr(self, key, value)
        except json.JSONDecodeError as e:
            logger.error(
                f"Ошибка декодирования JSON при загрузке состояния: {e}"
            )
            self._reset()
        except Exception as e:
            logger.error(
                f"Непредвиденная ошибка при загрузке состояния: {type(e).__name__}: {e}"
            )
            self._reset()

    async def _get_photo(self) -> Optional[types.Photo]:
        """Получение фотографии профиля из указанного сообщения."""
        if not self.running:
            return None
        try:
            message = await self._client.get_messages(
                self.chat_id, ids=self.message_id
            )
            if not message or not message.photo:
                await self._stop()
                return None
            return message.photo
        except MessageIdInvalidError:
            await self._stop()
            return None
        except Exception as e:
            logger.error(f"Ошибка получения фото: {e}")
            return None

    async def _set_profile_photo(
        self, photo: types.Photo
    ) -> Union[bool, errors.FloodWaitError, Exception]:
        """Обновление фотографии профиля."""
        try:
            await self._client(
                functions.photos.UpdateProfilePhotoRequest(
                    id=types.InputPhoto(
                        id=photo.id,
                        access_hash=photo.access_hash,
                        file_reference=photo.file_reference,
                    )
                )
            )
            return True
        except errors.FloodWaitError as e:
            return e
        except (
            PhotoInvalidDimensionsError,
            PhotoCropSizeSmallError,
            PhotoSaveFileInvalidError,
        ) as e:
            return e
        except Exception as e:
            logger.error(f"Ошибка при обновлении фото профиля: {e}")
            return e

    async def _handle_flood_wait(self, error: errors.FloodWaitError):
        """Обработка ошибки FloodWait."""
        self.flood_count += 1
        self.floods.append(datetime.now())
        self.success_streak = 0
        self.delay = min(
            self.config[CONFIG_MAX_DELAY],
            self.delay * self.config[CONFIG_DELAY_MULTIPLIER],
        )
        if self.config[CONFIG_NOTIFY_ERRORS]:
            await self._client.send_message(
                self.chat_id,
                self.strings["flood_wait"].format(
                    delay=self.delay / 60, wait=error.seconds / 60
                ),
            )
        logger.warning(
            f"Получен FloodWait: {error.seconds}s. Новая задержка: {self.delay}s"
        )
        await asyncio.sleep(error.seconds)

    async def _handle_photo_invalid_error(self, error):
        """Обработка ошибок, связанных с неверным форматом фотографии."""
        self.error_count += 1
        self.last_error_time = datetime.now()
        if self.config[CONFIG_NOTIFY_ERRORS]:
            await self._client.send_message(
                self.chat_id,
                self.strings["photo_invalid"].format(error=str(error)),
            )
        await self._stop()

    async def _handle_generic_error(self, error):
        """Обработка общих ошибок при обновлении фотографии профиля."""
        self.error_count += 1
        self.success_streak = 0
        self.retries += 1
        self.last_error_time = datetime.now()
        if self.config[CONFIG_NOTIFY_ERRORS]:
            await self._client.send_message(
                self.chat_id, self.strings["error"].format(error=str(error))
            )
        logger.error(f"Ошибка при обновлении фото: {error}")

    async def _process_set_photo_result(
        self, result: Union[bool, errors.FloodWaitError, Exception]
    ) -> bool:
        """Обработка результата попытки обновления фотографии профиля."""
        if result is True:
            self.last_update = datetime.now()
            self.update_count += 1
            self.success_streak += 1
            self.retries = 0
            self._save_state()
            logger.info(
                f"Фото профиля обновлено. Обновлений: {self.update_count}, "
                f"Текущая серия успехов: {self.success_streak}"
            )
            return True
        elif isinstance(result, errors.FloodWaitError):
            await self._handle_flood_wait(result)
            return False
        elif isinstance(
            result,
            (
                PhotoInvalidDimensionsError,
                PhotoCropSizeSmallError,
                PhotoSaveFileInvalidError,
            ),
        ):
            await self._handle_photo_invalid_error(result)
            return False
        else:
            await self._handle_generic_error(result)
            return False

    async def _update(self) -> bool:
        """Попытка обновить фотографию профиля."""
        try:
            if not self.running:
                return False
            photo = await self._get_photo()
            if not photo:
                return False
            result = await self._set_profile_photo(photo)
            return await self._process_set_photo_result(result)
        except Exception as e:
            await self._handle_generic_error(e)
            return False
        finally:
            photo = None

    def _calculate_delay(self) -> float:
        """Calculates the delay with optimized and randomized intervals."""
        if not self.config[CONFIG_ADAPTIVE_DELAY]:
            return self.delay
        base_delay = self.delay
        now = datetime.now()

        if self.success_streak >= 5:
            base_delay *= self.config[CONFIG_SUCCESS_REDUCTION]
        available_ranges = [
            r
            for r in self.multiplier_ranges
            if r not in self.recent_multipliers
        ]
        if not available_ranges:
            selected_range = random.choice(self.multiplier_ranges)
        else:
            selected_range = random.choice(available_ranges)
        base_multiplier = random.uniform(selected_range[0], selected_range[1])
        self.recent_multipliers.append(selected_range)

        jitter = random.gauss(1.0, self.config[CONFIG_JITTER])
        jitter = max(
            1 - self.config[CONFIG_JITTER],
            min(1 + self.config[CONFIG_JITTER], jitter),
        )

        delay = base_delay * base_multiplier * jitter

        if (
            self.last_error_time
            and (now - self.last_error_time).total_seconds()
            < self.config[CONFIG_MAX_DELAY]
        ):
            delay *= self.config[CONFIG_DELAY_MULTIPLIER]
        if self.floods:
            recent_floods = len(self.floods)
            delay = min(
                self.config[CONFIG_MAX_DELAY],
                delay * (self.config[CONFIG_DELAY_MULTIPLIER] ** recent_floods),
            )
        self.total_updates_cycle += 1

        logger.info(
            f"Success streak: {self.success_streak}, Current delay: {delay:.2f} seconds"
        )

        return max(
            self.config[CONFIG_MIN_DELAY],
            min(self.config[CONFIG_MAX_DELAY], delay),
        )

    async def _loop(self) -> None:
        """Основной асинхронный цикл для периодического обновления фотографии."""
        while self.running:
            try:
                now = datetime.now()
                calculated_delay = self._calculate_delay()

                if self.last_update:
                    elapsed = (now - self.last_update).total_seconds()
                    if elapsed < calculated_delay:
                        await asyncio.sleep(calculated_delay - elapsed)
                if await self._update():
                    await asyncio.sleep(calculated_delay)
                else:
                    if self.retries >= self.config[CONFIG_ERROR_THRESHOLD]:
                        await self._stop()
                        break
                    await asyncio.sleep(self.config[CONFIG_MIN_DELAY])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Ошибка в цикле: {type(e).__name__}: {e}")
                await asyncio.sleep(self.config[CONFIG_MIN_DELAY])

    def _format_time(self, seconds: float) -> str:
        """Форматирование времени в человекочитаемый вид."""
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{int(hours)}ч {int(minutes)}м"
        elif minutes:
            return f"{int(minutes)}м {int(round(seconds))}с"
        return f"{int(round(seconds))}с"

    def _get_delay_details(self) -> str:
        """Формирование информации о факторах, влияющих на задержку."""
        details = []
        now = datetime.now()

        if self.success_streak >= 5:
            details.append(self.strings["delay_details_success"])
        recent_flood = any(
            (now - flood_time).total_seconds() < 3600
            for flood_time in self.floods
        )
        if recent_flood:
            details.append(self.strings["delay_details_recent_flood"])
        if (
            self.last_error_time
            and (now - self.last_error_time).total_seconds()
            < self.config[CONFIG_MAX_DELAY]
        ):
            details.append(self.strings["delay_details_recent_error"])
        if self.config[CONFIG_JITTER] > 0:
            details.append(
                self.strings["delay_details_jitter"].format(
                    jitter_percent=self.config[CONFIG_JITTER] * 100
                )
            )
        return (
            "\n".join(details)
            if details
            else "  • Нет активных факторов адаптации"
        )

    def _get_stats(self) -> Dict[str, str]:
        """Получение статистики работы модуля."""
        now = datetime.now()
        uptime = (
            (now - self.start_time).total_seconds() if self.start_time else 0
        )
        last = (
            (now - self.last_update).total_seconds() if self.last_update else 0
        )

        return {
            "status": "✅ Работает" if self.running else "🛑 Остановлен",
            "uptime": self._format_time(uptime),
            "count": str(self.update_count),
            "hourly": (
                f"{self.update_count / (uptime/3600):.1f}"
                if uptime > 0
                else "0"
            ),
            "delay": self.delay / 60,
            "last": self._format_time(last) if self.last_update else "никогда",
            "errors": str(self.error_count),
            "floods": str(self.flood_count),
            "delay_details": self._get_delay_details(),
        }

    def _save_state(self):
        """Сохранение текущего состояния модуля в базу данных."""
        try:
            self._db.set(
                self.strings["name"], "state", json.dumps(self._get_state())
            )
        except TypeError as e:
            logger.error(f"Ошибка сериализации состояния: {e}")

    async def _start(self, chat_id: int, message_id: int) -> None:
        """Запуск процесса автоматической смены фотографии."""
        self._reset()
        self.running = True
        self.start_time = datetime.now()
        self.chat_id = chat_id
        self.message_id = message_id
        self._save_state()
        self._task = asyncio.create_task(self._loop())
        logger.info("Profile changer started")

    async def _send_stopping_message(self):
        """Отправка сообщения об остановке процесса."""
        await self._client.send_message(
            self.chat_id,
            self.strings["stopping"].format(
                count=self.update_count,
                uptime=(
                    self._format_time(
                        (datetime.now() - self.start_time).total_seconds()
                    )
                    if self.start_time
                    else "0с"
                ),
                errors=self.error_count,
            ),
        )

    async def _stop(self) -> None:
        """Остановка процесса автоматической смены фотографии."""
        if not self.running:
            return
        self.running = False

        try:
            if self._task and not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            await asyncio.get_event_loop().run_in_executor(
                None, self._save_state
            )
            await self._send_stopping_message()

            self._reset()

            logger.info("Profile changer stopped successfully")
        except Exception as e:
            logger.error(f"Ошибка при остановке Profile changer: {e}")
            self._reset()

    def _sort_photos(self, photos: List[str]) -> List[str]:
        """Сортировка фотографий по номеру в имени файла в обратном порядке."""

        def extract_number(filename):
            match = re.search(r"frame-(\d+)", filename)
            return int(match.group(1)) if match else 0

        return sorted(photos, key=extract_number, reverse=True)

    async def _upload_photo(self, path: str) -> Union[bool, Exception]:
        """Загрузка одной фотографии на профиль."""
        try:
            await self._client(
                functions.photos.UploadProfilePhotoRequest(
                    file=await self._client.upload_file(path)
                )
            )
            return True
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds)
            return e
        except Exception as e:
            return e

    @loader.command()
    async def pfp(self, message):
        """Запустить смену фото профиля (ответьте на сообщение с фото)."""
        async with self._lock:
            if self.running:
                await utils.answer(message, self.strings["already_running"])
                return
            target = (
                await message.get_reply_message()
                if message.is_reply
                else message
            )
            photo_entity = target.photo if target else None

            if not photo_entity:
                await utils.answer(message, self.strings["no_photo"])
                return
            await self._start(message.chat_id, target.id)
            await utils.answer(
                message,
                self.strings["starting"].format(
                    delay_minutes=round(self.delay / 60),
                    updates_per_hour=round(3600 / self.delay),
                ),
            )

    @loader.command()
    async def pfpstop(self, message):
        """Остановить смену фото профиля."""
        try:
            async with self._lock:
                if not self.running:
                    await utils.answer(message, self.strings["not_running"])
                    return
                await utils.answer(message, self.strings["stopping_timeout"])
                await asyncio.wait_for(self._stop(), timeout=9)
            await utils.answer(message, self.strings["stopped_successfully"])
        except asyncio.TimeoutError:
            await utils.answer(message, self.strings["stopping_timeout"])
            await utils.answer(
                message,
                "❌ <b>Превышено время ожидания остановки. Попробуйте позже.</b>",
            )
            self._reset()
        except Exception as e:
            await utils.answer(message, f"❌ Ошибка при остановке: {str(e)}")
            logger.error(f"Ошибка в pfpstop: {e}")
            self._reset()

    @loader.command()
    async def pfpstats(self, message):
        """Показать статистику работы модуля."""
        await utils.answer(
            message, self.strings["stats"].format(**self._get_stats())
        )

    @loader.command()
    async def pfpdelay(self, message):
        """Установить задержку между обновлениями в секундах.

        Используйте: .pfpdelay <секунды>
        """
        args = utils.get_args_raw(message)

        if not args:
            return await utils.answer(
                message, "Укажите задержку в секундах после команды."
            )
        try:
            delay = float(args)
            if (
                delay < self.config["min_delay"]
                or delay > self.config["max_delay"]
            ):
                return await utils.answer(
                    message,
                    f"Задержка должна быть от {self.config['min_delay']} до {self.config['max_delay']} секунд",
                )
            self.delay = delay
            self._save_state()
            await utils.answer(
                message, f"✅ Установлена задержка {delay} секунд"
            )
        except ValueError:
            await utils.answer(
                message, "❌ Неверный формат числа. Введите число секунд."
            )

    @loader.command()
    async def pfpdir(self, message):
        """Загрузить фотографии из директории /root/Heroku/new.

        Использование: .pfpdir <задержка_в_секундах>
        Пример: .pfpdir 60
        """
        args = utils.get_args(message)
        if len(args) != 1:
            await utils.answer(message, "ℹ️ Использование: .pfpdir <задержка>")
            return

        directory = "/root/Heroku/new"
        try:
            delay = float(args[0])
            if delay < 0:
                raise ValueError
        except ValueError:
            await utils.answer(message, self.strings["invalid_delay"])
            return

        if not os.path.isdir(directory):
            await utils.answer(message, self.strings["dir_not_found"])
            return

        photos = [
            f
            for f in os.listdir(directory)
            if f.startswith("ezgif-frame-") and f.endswith(".jpg")
        ]

        if not photos:
            await utils.answer(message, self.strings["no_photos"])
            return

        photos = self._sort_photos(photos)

        status_message = await utils.answer(
            message,
            self.strings["loading_from_dir"].format(
                count=len(photos), delay=delay
            ),
        )

        start_time = datetime.now()
        uploaded = 0
        errors = 0
        total_photos = len(photos)

        for index, photo in enumerate(photos, 1):
            photo_path = os.path.join(directory, photo)

            await utils.answer(
                message,
                self.strings["current_photo"].format(
                    photo=photo, current=index, total=total_photos
                ),
            )

            result = await self._upload_photo(photo_path)

            if isinstance(result, bool) and result:
                uploaded += 1
            else:
                errors += 1
                logger.error(f"Error uploading {photo}: {result}")

            await asyncio.sleep(delay)

        elapsed = datetime.now() - start_time
        await utils.answer(
            message,
            self.strings["dir_complete"].format(
                uploaded=uploaded,
                errors=errors,
                elapsed=self._format_time(elapsed.total_seconds()),
            ),
        )

    @loader.command()
    async def pfpon(self, message):
        """Установить аватарку один раз (ответьте на сообщение с фото)."""
        reply = await message.get_reply_message()
        if not reply or not reply.photo:
            await utils.answer(message, self.strings["pfpone_no_reply"])
            return
        photo = reply.photo
        result = await self._set_profile_photo(photo)
        await self._process_set_photo_result(result)
