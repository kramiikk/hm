"""Main bot page"""
import asyncio
import collections
import functools
import logging
import os
import re
import string
import time

import aiohttp_jinja2
import requests
from aiohttp import web
from hikkatl.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    YouBlockedUserError,
)
from hikkatl.password import compute_check
from hikkatl.sessions import MemorySession
from hikkatl.tl.functions.account import GetPasswordRequest
from hikkatl.tl.functions.auth import CheckPasswordRequest
from hikkatl.tl.functions.contacts import UnblockRequest
from hikkatl.utils import parse_phone

from .. import database, main, utils
from .._internal import restart
from ..tl_cache import CustomTelegramClient
from ..version import __version__

DATA_DIR = (
    "/data"
    if "DOCKER" in os.environ
    else os.path.normpath(os.path.join(utils.get_base_dir(), ".."))
)

logger = logging.getLogger(__name__)


class Web:
    def __init__(self, **kwargs):
        self.sign_in_clients = {}
        self._pending_client = None
        self._qr_login = None
        self._qr_task = None
        self._2fa_needed = None
        self._sessions = []
        self._ratelimit = {}
        self.api_token = kwargs.pop("api_token")
        self.data_root = kwargs.pop("data_root")
        self.connection = kwargs.pop("connection")
        self.proxy = kwargs.pop("proxy")

        self.app.router.add_get("/", self.root)
        self.app.router.add_put("/set_api", self.set_tg_api)
        self.app.router.add_post("/send_tg_code", self.send_tg_code)
        self.app.router.add_post("/check_session", self.check_session)
        self.app.router.add_post("/tg_code", self.tg_code)
        self.app.router.add_post("/finish_login", self.finish_login)
        self.app.router.add_post("/init_qr_login", self.init_qr_login)
        self.app.router.add_post("/get_qr_url", self.get_qr_url)
        self.app.router.add_post("/qr_2fa", self.qr_2fa)
        self.app.router.add_post("/can_add", self.can_add)
        self.api_set = asyncio.Event()
        self.clients_set = asyncio.Event()

    @property
    def _platform_emoji(self) -> str:
        return {
            "vds": "https://github.com/hikariatama/assets/raw/master/waning-crescent-moon_1f318.png",
            "termux": "https://github.com/hikariatama/assets/raw/master/smiling-face-with-sunglasses_1f60e.png",
            "docker": "https://github.com/hikariatama/assets/raw/master/spouting-whale_1f433.png",
        }[
            (
                "termux"
                if "com.termux" in os.environ.get("PREFIX", "")
                else "docker"
                if "DOCKER" in os.environ
                else "vds"
            )
        ]

    @aiohttp_jinja2.template("root.jinja2")
    async def root(self, _):
        return {
            "skip_creds": self.api_token is not None,
            "tg_done": bool(self.client_data),
            "platform_emoji": self._platform_emoji,
        }

    async def check_session(self, request: web.Request) -> web.Response:
        return web.Response(body=("1" if self._check_session(request) else "0"))

    def wait_for_api_token_setup(self):
        return self.api_set.wait()

    def wait_for_clients_setup(self):
        return self.clients_set.wait()

    def _check_session(self, request: web.Request) -> bool:
        return (
            request.cookies.get("session", None) in self._sessions
            if main.hikka.clients
            else True
        )

    async def set_tg_api(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401, body="Authorization required")

        text = await request.text()

        if len(text) < 36:
            return web.Response(
                status=400,
                body="API ID and HASH pair has invalid length",
            )

        api_id = text[32:]
        api_hash = text[:32]

        if any(c not in string.hexdigits for c in api_hash) or any(
            c not in string.digits for c in api_id
        ):
            return web.Response(
                status=400,
                body="You specified invalid API ID and/or API HASH",
            )

        main.save_config_key("api_id", int(api_id))
        main.save_config_key("api_hash", api_hash)

        self.api_token = collections.namedtuple("api_token", ("ID", "HASH"))(
            api_id,
            api_hash,
        )

        self.api_set.set()
        return web.Response(body="ok")

    async def _qr_login_poll(self):
        logged_in = False
        self._2fa_needed = False
        logger.debug("Waiting for QR login to complete")
        while not logged_in:
            try:
                logged_in = await self._qr_login.wait(10)
            except asyncio.TimeoutError:
                logger.debug("Recreating QR login")
                try:
                    await self._qr_login.recreate()
                except SessionPasswordNeededError:
                    self._2fa_needed = True
                    return
            except SessionPasswordNeededError:
                self._2fa_needed = True
                break

        logger.debug("QR login completed. 2FA needed: %s", self._2fa_needed)
        self._qr_login = True

    async def init_qr_login(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401)

        if self._pending_client is not None:
            self._pending_client = None
            self._qr_login = None
            if self._qr_task:
                self._qr_task.cancel()
                self._qr_task = None

            self._2fa_needed = False
            logger.debug("QR login cancelled, new session created")

        client = self._get_client()
        self._pending_client = client

        await client.connect()
        self._qr_login = await client.qr_login()
        self._qr_task = asyncio.ensure_future(self._qr_login_poll())

        return web.Response(body=self._qr_login.url)

    async def get_qr_url(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401)

        if self._qr_login is True:
            if self._2fa_needed:
                return web.Response(status=403, body="2FA")

            await main.hikka.save_client_session(self._pending_client)
            return web.Response(status=200, body="SUCCESS")

        if self._qr_login is None:
            await self.init_qr_login(request)

        if self._qr_login is None:
            return web.Response(
                status=500,
                body="Internal Server Error: Unable to initialize QR login",
            )

        return web.Response(status=201, body=self._qr_login.url)

    def _get_client(self) -> CustomTelegramClient:
        return CustomTelegramClient(
            MemorySession(),
            self.api_token.ID,
            self.api_token.HASH,
            connection=self.connection,
            proxy=self.proxy,
            connection_retries=None,
            device_model=main.get_app_name(),
            system_version=main.generate_random_system_version(),
            app_version=".".join(map(str, __version__)) + " x64",
            lang_code="en",
            system_lang_code="en-US",
        )

    async def can_add(self, request: web.Request) -> web.Response:
        return web.Response(status=200, body="Yes")

    async def send_tg_code(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401, body="Authorization required")

        if self.client_data and "LAVHOST" in os.environ:
            return web.Response(status=403, body="Forbidden by host EULA")

        if self._pending_client:
            return web.Response(status=208, body="Already pending")

        text = await request.text()
        phone = parse_phone(text)

        if not phone:
            return web.Response(status=400, body="Invalid phone number")

        client = self._get_client()

        self._pending_client = client

        await client.connect()
        try:
            await client.send_code_request(phone)
        except FloodWaitError as e:
            return web.Response(status=429, body=self._render_fw_error(e))

        return web.Response(body="ok")

    @staticmethod
    def _render_fw_error(e: FloodWaitError) -> str:
        seconds, minutes, hours = (
            e.seconds % 3600 % 60,
            e.seconds % 3600 // 60,
            e.seconds // 3600,
        )
        seconds, minutes, hours = (
            f"{seconds} second(-s)",
            f"{minutes} minute(-s) " if minutes else "",
            f"{hours} hour(-s) " if hours else "",
        )
        return (
            f"You got FloodWait for {hours}{minutes}{seconds}. Wait the specified"
            " amount of time and try again."
        )

    async def qr_2fa(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401)

        text = await request.text()

        logger.debug("2FA code received for QR login: %s", text)

        try:
            await self._pending_client._on_login(
                (
                    await self._pending_client(
                        CheckPasswordRequest(
                            compute_check(
                                await self._pending_client(GetPasswordRequest()),
                                text.strip(),
                            )
                        )
                    )
                ).user
            )
        except PasswordHashInvalidError:
            logger.debug("Invalid 2FA code")
            return web.Response(
                status=403,
                body="Invalid 2FA password",
            )
        except FloodWaitError as e:
            logger.debug("FloodWait for 2FA code")
            return web.Response(
                status=421,
                body=(self._render_fw_error(e)),
            )

        logger.debug("2FA code accepted, logging in")
        await main.hikka.save_client_session(self._pending_client)
        return web.Response()

    async def tg_code(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401)

        text = await request.text()

        if len(text) < 6:
            return web.Response(status=400)

        split = text.split("\n", 2)

        if len(split) not in (2, 3):
            return web.Response(status=400)

        code = split[0]
        phone = parse_phone(split[1])
        password = split[2]

        if (
            (len(code) != 5 and not password)
            or any(c not in string.digits for c in code)
            or not phone
        ):
            return web.Response(status=400)

        if not password:
            try:
                await self._pending_client.sign_in(phone, code=code)
            except SessionPasswordNeededError:
                return web.Response(
                    status=401,
                    body="2FA Password required",
                )
            except PhoneCodeExpiredError:
                return web.Response(status=404, body="Code expired")
            except PhoneCodeInvalidError:
                return web.Response(status=403, body="Invalid code")
            except FloodWaitError as e:
                return web.Response(
                    status=421,
                    body=(self._render_fw_error(e)),
                )
        else:
            try:
                await self._pending_client.sign_in(phone, password=password)
            except PasswordHashInvalidError:
                return web.Response(
                    status=403,
                    body="Invalid 2FA password",
                )
            except FloodWaitError as e:
                return web.Response(
                    status=421,
                    body=(self._render_fw_error(e)),
                )

        await main.hikka.save_client_session(self._pending_client)
        return web.Response()

    async def finish_login(self, request: web.Request) -> web.Response:
        if not self._check_session(request):
            return web.Response(status=401)

        if not self._pending_client:
            return web.Response(status=400)

        first_session = not bool(main.hikka.clients)

        main.hikka.clients = list(set(main.hikka.clients + [self._pending_client]))
        self._pending_client = None

        self.clients_set.set()

        if not first_session:
            restart()

        return web.Response()
