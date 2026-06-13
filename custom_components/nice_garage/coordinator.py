"""
NiceHub — manages the live device connections for one config entry.
"""
from __future__ import annotations

import asyncio
import logging
from time import monotonic

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .api import NhkClient, NiceApiError, NiceCloud, parse_door_statuses
from .const import (
    KEEPALIVE_INTERVAL,
    RECONNECT_BACKOFF,
    SIGNAL_AVAILABLE,
    SIGNAL_STATE,
)

_LOGGER = logging.getLogger(__name__)


class NiceHub:
    """Owns discovery results and the single live socket for an account."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        cloud: NiceCloud,
        doors: list[dict],
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.cloud = cloud
        self.doors = doors

        self._stopping = False
        self._task: asyncio.Task | None = None
        self._client = NhkClient()
        self._states: dict[int, str] = {}  # automation_id -> last DoorStatus
        self._available: dict[int, bool] = {}  # automation_id -> session up?

        # mac -> {device_id -> automation_id} routing table, and mac -> creds
        self._route: dict[str, dict[str, int]] = {}
        self._creds: dict[str, dict] = {}
        for d in doors:
            mac = d.get("mac")
            if mac and d.get("creds"):
                self._route.setdefault(mac, {})[d["device_id"]] = d["automation_id"]
                self._creds[mac] = d["creds"]

    # --- lifecycle --------------------------------------------------------- #
    async def async_start(self) -> None:
        """Spawn the single connection task (if any accessory is controllable)."""
        if not self._creds:
            return
        self._task = self.entry.async_create_background_task(
            self.hass, self._run(), "nice_garage_connection"
        )

    async def async_stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        await self._client.close()

    # --- state access for entities ---------------------------------------- #
    def state_for(self, automation_id: int) -> str | None:
        return self._states.get(automation_id)

    def available_for(self, automation_id: int) -> bool:
        return self._available.get(automation_id, False)

    # --- commands ---------------------------------------------------------- #
    async def async_door_action(self, door: dict, action: str) -> None:
        """Send open/close/stop for a door over the shared socket."""
        mac = door.get("mac")
        if not self._client.connected or not self._client.has_session(mac):
            raise NiceApiError(f"device {mac} is not connected")
        await self._client.send_change(mac, action)

    # --- internals --------------------------------------------------------- #
    def _set_available(self, mac: str, available: bool) -> None:
        for automation_id in self._route.get(mac, {}).values():
            self._available[automation_id] = available
            async_dispatcher_send(
                self.hass, SIGNAL_AVAILABLE.format(automation_id), available
            )

    def _publish(self, mac: str, statuses: dict[str, str]) -> None:
        route = self._route.get(mac, {})
        for dev_id, status in statuses.items():
            automation_id = route.get(dev_id)
            if automation_id is None and len(route) == 1:
                # single door on this accessory: accept status regardless of id
                automation_id = next(iter(route.values()))
            if automation_id is None:
                continue
            if self._states.get(automation_id) != status:
                _LOGGER.debug("Nice door %s -> %s", automation_id, status)
            self._states[automation_id] = status
            async_dispatcher_send(
                self.hass, SIGNAL_STATE.format(automation_id), status
            )

    async def _connect_all(self) -> int:
        """Open the socket and CONNECT every accessory (best-effort).

        Returns the number of accessories that connected. One accessory failing
        does not prevent the others.
        """
        await self._client.open()
        # Pass 1: CONNECT every accessory (best-effort) before priming any
        # STATUS, so one accessory's status reply isn't swallowed while another
        # is still doing its CONNECT handshake.
        for mac, creds in self._creds.items():
            try:
                await self._client.add_session(
                    mac, creds["user"], creds["password"], creds["controller"]
                )
            except (NiceApiError, asyncio.TimeoutError, OSError) as err:
                _LOGGER.warning("Nice: could not connect accessory %s: %s", mac, err)
                self._set_available(mac, False)
        # Pass 2: prime initial state for everything that connected
        for mac in list(self._client.sessions):
            self._set_available(mac, True)
            await self._client.send_status(mac)
        return len(self._client.sessions)

    async def _run(self) -> None:
        """Maintain the shared socket, reconnecting all sessions on failure."""
        while not self._stopping:
            try:
                if await self._connect_all() == 0:
                    raise NiceApiError("no accessories connected")
                last_ka = monotonic()
                while not self._stopping:
                    frame = await self._client.read_frame(timeout=5.0)
                    if frame:
                        if "<Error" in frame:
                            _LOGGER.debug("NHK error frame: %s", frame.strip())
                        mac = self._client.route(frame)
                        if mac:
                            statuses = parse_door_statuses(frame)
                            if statuses:
                                self._publish(mac, statuses)
                    if monotonic() - last_ka >= KEEPALIVE_INTERVAL:
                        for mac in list(self._client.sessions):
                            await self._client.send_status(mac)
                        last_ka = monotonic()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Nice connection lost: %s", err)
            finally:
                for mac in self._creds:
                    self._set_available(mac, False)
                await self._client.close()
            if not self._stopping:
                await asyncio.sleep(RECONNECT_BACKOFF)
