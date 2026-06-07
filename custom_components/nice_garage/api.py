"""Async client for the Nice/MyNice cloud + NHK device socket."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import random
import re
import ssl
import time
from typing import Any, Callable

import aiohttp

from .const import (
    BASE_URL,
    CLIENT_ID,
    CLIENT_SECRET,
    COMMON_HEADERS,
    ETX,
    PROXY_HOST,
    PROXY_PORT,
    STX,
)

_LOGGER = logging.getLogger(__name__)


class NiceAuthError(Exception):
    """Authentication failed (bad credentials / refresh rejected)."""


class NiceApiError(Exception):
    """A cloud or device request failed."""


# --------------------------------------------------------------------------- #
# small crypto / encoding helpers (mirror com.niceforyou.nhk.util.Algorithms)
# --------------------------------------------------------------------------- #
def _sha256(*parts: bytes) -> bytes:
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _attr(xml: str, name: str) -> str | None:
    m = re.search(name + r'\s*=\s*"([^"]*)"', xml or "")
    return m.group(1) if m else None


def _tag(xml: str, name: str) -> str | None:
    m = re.search(r"<" + name + r">([^<]*)</" + name + r">", xml or "")
    return m.group(1) if m else None


def parse_door_statuses(xml: str) -> dict[str, str]:
    """Return {device_id: DoorStatus} for every Device block in a frame.

    Handles both single-device replies and (defensively) multi-device frames.
    Falls back to a bare DoorStatus with device id "1" if no Device id is found.
    """
    out: dict[str, str] = {}
    for dev_id, block in re.findall(
        r'<Device id="([^"]+)">(.*?)</Device>', xml or "", re.DOTALL
    ):
        ds = _tag(block, "DoorStatus")
        if ds:
            out[dev_id] = ds
    if not out:
        ds = _tag(xml, "DoorStatus")
        if ds:
            out["1"] = ds
    return out


def _ssl_context() -> ssl.SSLContext:
    """Trust-all context mirroring the app's X509 manager (device cert is loose)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# --------------------------------------------------------------------------- #
# Cloud REST layer: OAuth2 login/refresh with in-memory token + persist hook
# --------------------------------------------------------------------------- #
class NiceCloud:
    """OAuth2 + discovery against the MyNice cloud.

    `on_token` (optional) is called with the token dict whenever it changes, so
    the caller can persist it (Home Assistant stores it in the config entry).
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        username: str | None = None,
        password: str | None = None,
        token: dict | None = None,
        on_token: Callable[[dict], None] | None = None,
    ) -> None:
        self._session = session
        self.username = username
        self.password = password
        self._token = dict(token) if token else None
        self._on_token = on_token

    @property
    def token(self) -> dict | None:
        return self._token

    def _basic(self) -> str:
        return "Basic " + _b64(f"{CLIENT_ID}:{CLIENT_SECRET}".encode())

    def _store_token(self, tok: dict) -> None:
        tok = dict(tok)
        tok["_expires_at"] = time.time() + int(tok.get("expires_in", 3600))
        self._token = tok
        if self._on_token:
            self._on_token(tok)

    async def _token_request(self, params: dict[str, str]) -> dict:
        try:
            async with self._session.post(
                BASE_URL + "oauth/token",
                params=params,
                headers={
                    **COMMON_HEADERS,
                    "Authorization": self._basic(),
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                body = await r.text()
                if r.status != 200:
                    raise NiceAuthError(f"token request failed ({r.status}): {body[:200]}")
                import json

                return json.loads(body)
        except aiohttp.ClientError as err:
            raise NiceApiError(f"network error during auth: {err}") from err

    async def async_login(self) -> None:
        """Full password grant. Requires username/password to be set."""
        if not self.username or not self.password:
            raise NiceAuthError("no credentials available for password login")
        _LOGGER.debug("Nice: password login")
        tok = await self._token_request(
            {
                "grant_type": "password",
                "username": self.username,
                "password": self.password,
            }
        )
        if not tok.get("access_token"):
            raise NiceAuthError("login response missing access_token")
        self._store_token(tok)

    async def async_refresh(self) -> bool:
        if not self._token or not self._token.get("refresh_token"):
            return False
        _LOGGER.debug("Nice: refreshing token")
        try:
            tok = await self._token_request(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": self._token["refresh_token"],
                }
            )
        except NiceAuthError:
            return False
        if not tok.get("access_token"):
            return False
        # refresh responses sometimes omit refresh_token; keep the old one
        tok.setdefault("refresh_token", self._token.get("refresh_token"))
        self._store_token(tok)
        return True

    async def async_ensure_token(self) -> None:
        """Authenticate only when there is no valid cached token."""
        t = self._token
        if t and t.get("access_token") and t.get("_expires_at", 0) > time.time() + 60:
            return
        if await self.async_refresh():
            return
        await self.async_login()

    async def _auth_headers(self) -> dict[str, str]:
        await self.async_ensure_token()
        tt = self._token.get("token_type", "Bearer")
        return {
            **COMMON_HEADERS,
            "Authorization": f"{tt} {self._token['access_token']}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, **kw: Any) -> Any:
        for attempt in (1, 2):
            headers = await self._auth_headers()
            try:
                async with self._session.request(
                    method,
                    BASE_URL + path,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                    **kw,
                ) as r:
                    if r.status == 401 and attempt == 1:
                        self._token = None  # force re-auth then retry once
                        continue
                    if r.status != 200:
                        text = await r.text()
                        raise NiceApiError(f"{path} failed ({r.status}): {text[:200]}")
                    return await r.json()
            except aiohttp.ClientError as err:
                raise NiceApiError(f"network error on {path}: {err}") from err
        raise NiceApiError(f"{path} failed after re-auth")

    async def async_discover(self) -> list[dict]:
        """Return a list of door dicts incl. per-accessory socket credentials."""
        me = (await self._request("GET", "api/v1/users/data-act/me"))["data"]
        sd = await self._request("POST", "api/v1/user/smartdeviceList")

        creds: dict[str, dict] = {}
        for dev in sd.get("smartDevices", []):
            for ac in dev.get("accessoryCredentials", []) or []:
                creds[ac["accessoryMacAddress"]] = {
                    "user": ac["accessoryUser"],
                    "password": ac["accessoryPassword"],
                    "controller": ac["controllerID"],
                }

        doors: list[dict] = []
        for home in me.get("homes", []):
            for amb in home.get("ambients", []):
                for a in amb.get("automations", []):
                    mac = a.get("accessoryMacAddress")
                    doors.append(
                        {
                            "automation_id": a.get("id"),
                            "device_id": str(a.get("deviceId") or "1"),
                            "name": a.get("automationName"),
                            "type": a.get("automationType"),
                            "model": a.get("automationModel"),
                            "mac": mac,
                            "online": a.get("statusAutomationNetwork"),
                            "creds": creds.get(mac),
                        }
                    )
        return doors


# --------------------------------------------------------------------------- #
# NHK device socket: async TLS + signed XML protocol (commands + status + events)
# --------------------------------------------------------------------------- #
class NhkConnection:
    """One async TLS session to the device proxy for a single accessory."""

    def __init__(self, mac: str, user: str, password: str, controller: str) -> None:
        self.mac = mac
        self.user = user
        self.password = password
        self.controller = controller
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._buf = bytearray()
        self.session_id: int | None = None
        self.session_pw: bytes | None = None
        self._counter = 1

    # ---- framing & signing (mirror NHKParseMessage / NHKConnection) -------
    def _next_id(self, with_session: bool = True) -> int:
        c = self._counter
        self._counter += 1
        if with_session and self.session_id is not None:
            return (self.session_id & 0xFF) | (c << 8)
        return c

    def _frame(self, xml: str, signed: bool) -> bytes:
        xml = xml.replace("\n", "\r\n")  # device expects CRLF
        if signed:
            i = xml.index("<Sign>")
            prefix = xml[:i]
            sign = _b64(_sha256(_sha256(prefix.encode()), self.session_pw))
            xml = prefix + "<Sign>" + sign + "</Sign>" + xml[i + len("<Sign></Sign>"):]
        return bytes([STX]) + xml.encode() + bytes([ETX])

    def _header(self, rtype: str, with_session: bool = True) -> str:
        # attributes are alphabetically ordered exactly like SimpleXML's output
        return (
            f'<Request gw="" id="{self._next_id(with_session)}" '
            f'protocolType="NHK" protocolVersion="1.0" '
            f'source="{self.controller}" target="{self.mac}" type="{rtype}">'
        )

    async def _read_frame(self, timeout: float = 10.0) -> str | None:
        """Read one STX..ETX frame from the stream, or None on timeout."""
        assert self._reader is not None
        try:
            while True:
                if STX in self._buf:
                    start = self._buf.index(STX)
                    end = self._buf.find(ETX, start)
                    if end != -1:
                        frame = bytes(self._buf[start + 1 : end])
                        del self._buf[: end + 1]
                        return frame.decode("utf-8", "replace")
                    if start > 0:
                        del self._buf[:start]
                chunk = await asyncio.wait_for(self._reader.read(4096), timeout)
                if not chunk:
                    raise NiceApiError("device socket closed")
                self._buf.extend(chunk)
        except asyncio.TimeoutError:
            return None

    # ---- connection / session handshake ----------------------------------
    async def connect(self) -> "NhkConnection":
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(PROXY_HOST, PROXY_PORT, ssl=_ssl_context()),
            timeout=15,
        )
        cc = "%08X" % random.getrandbits(32)
        xml = (
            self._header("CONNECT", with_session=False)
            + f'\n   <Authentication cc="{cc}" username="{self.user}"/>\n</Request>'
        )
        self._writer.write(self._frame(xml, signed=False))
        await self._writer.drain()

        resp = await self._read_frame()
        sc = _attr(resp or "", "sc")
        if not resp or sc is None:
            raise NiceApiError(f"NHK CONNECT failed: {resp!r}")
        self.session_id = int(_attr(resp, "id"))
        # sessionPassword = sha256( pwd || reverse(sc) || reverse(cc) )
        self.session_pw = _sha256(
            bytes.fromhex(self.password),
            bytes.fromhex(sc)[::-1],
            bytes.fromhex(cc)[::-1],
        )
        _LOGGER.debug("NHK connected to %s (session %s)", self.mac, self.session_id)
        return self

    async def close(self) -> None:
        w = self._writer
        self._reader = self._writer = None
        if w is not None:
            try:
                w.close()
                await w.wait_closed()
            except (OSError, asyncio.CancelledError):
                pass

    @property
    def connected(self) -> bool:
        return self._writer is not None

    # ---- requests --------------------------------------------------------
    async def _send(self, rtype: str, body: str = "") -> None:
        assert self._writer is not None
        xml = self._header(rtype) + body + "\n   <Sign></Sign>\n</Request>"
        self._writer.write(self._frame(xml, signed=True))
        await self._writer.drain()

    async def request_status(self) -> None:
        """Ask the device to (re)report status; reply arrives as a frame."""
        await self._send("STATUS")

    async def status(self) -> str | None:
        """One-shot: send STATUS and return the current DoorStatus string."""
        await self._send("STATUS")
        for _ in range(6):  # skip async event frames, find the STATUS reply
            r = await self._read_frame(10)
            if r is None:
                break
            statuses = parse_door_statuses(r)
            if statuses:
                return next(iter(statuses.values()))
        return None

    async def send_door_action(self, action: str) -> None:
        """Write an open / close / stop command (does not read the reply).

        Used by the hub's listener loop, which is the sole reader of the socket;
        the resulting state change arrives as a pushed event frame.
        """
        if action not in ("open", "close", "stop"):
            raise ValueError(action)
        body = (
            '\n   <Devices>\n      <Devices>\n         <Device id="1">\n'
            "            <Services>\n               <DoorAction>"
            + action
            + "</DoorAction>\n            </Services>\n         </Device>\n"
            "      </Devices>\n   </Devices>"
        )
        await self._send("CHANGE", body)

    async def door_action(self, action: str) -> None:
        """Send open / close / stop and wait for the device's reply (one-shot use)."""
        await self.send_door_action(action)
        r = await self._read_frame(10)
        if r is None:
            raise NiceApiError("no response to CHANGE command")
        if "<Error" in r:
            raise NiceApiError(f"device rejected command: {r.strip()}")

    async def read_event(self, timeout: float) -> str | None:
        """Read the next frame (pushed event or reply); None on timeout."""
        return await self._read_frame(timeout)
