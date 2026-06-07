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

from .api import NhkConnection, NiceApiError, NiceCloud, parse_door_statuses
from .const import (
    KEEPALIVE_INTERVAL,
    RECONNECT_BACKOFF,
    SIGNAL_AVAILABLE,
    SIGNAL_STATE,
)

_LOGGER = logging.getLogger(__name__)


class NiceHub:
    """Owns discovery results and the live socket(s) for an account."""

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
        self._tasks: list[asyncio.Task] = []
        self._conns: dict[str, NhkConnection] = {}  # mac -> connection
        self._states: dict[int, str] = {}  # automation_id -> last DoorStatus
        self._available: dict[int, bool] = {}  # automation_id -> socket up?

        # mac -> {device_id -> automation_id} routing table
        self._route: dict[str, dict[str, int]] = {}
        for d in doors:
            mac = d.get("mac")
            if mac and d.get("creds"):
                self._route.setdefault(mac, {})[d["device_id"]] = d["automation_id"]

    # --- lifecycle --------------------------------------------------------- #
    async def async_start(self) -> None:
        """Spawn one listener task per accessory that has socket credentials."""
        for mac, door_map in self._route.items():
            door = next(d for d in self.doors if d["mac"] == mac)
            self._tasks.append(
                self.entry.async_create_background_task(
                    self.hass,
                    self._run_accessory(mac, door["creds"]),
                    f"nice_garage_{mac}",
                )
            )

    async def async_stop(self) -> None:
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()
        for conn in list(self._conns.values()):
            await conn.close()
        self._conns.clear()

    # --- state access for entities ---------------------------------------- #
    def state_for(self, automation_id: int) -> str | None:
        return self._states.get(automation_id)

    def available_for(self, automation_id: int) -> bool:
        return self._available.get(automation_id, False)

    # --- commands ---------------------------------------------------------- #
    async def async_door_action(self, door: dict, action: str) -> None:
        """Send open/close/stop for a door over its (already open) socket."""
        mac = door.get("mac")
        conn = self._conns.get(mac)
        if conn is None or not conn.connected:
            raise NiceApiError(f"device {mac} is not connected")
        await conn.send_door_action(action)

    # --- internals --------------------------------------------------------- #
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

    def _set_available(self, mac: str, available: bool) -> None:
        for automation_id in self._route.get(mac, {}).values():
            self._available[automation_id] = available
            async_dispatcher_send(
                self.hass, SIGNAL_AVAILABLE.format(automation_id), available
            )

    async def _run_accessory(self, mac: str, creds: dict) -> None:
        """Maintain the socket for one accessory, reconnecting on failure."""
        while not self._stopping:
            conn = NhkConnection(
                mac, creds["user"], creds["password"], creds["controller"]
            )
            try:
                await conn.connect()
                self._conns[mac] = conn
                self._set_available(mac, True)
                await conn.request_status()  # prime initial state
                last_ka = monotonic()
                while not self._stopping:
                    frame = await conn.read_event(timeout=5.0)
                    if frame:
                        if "<Error" in frame:
                            _LOGGER.warning("NHK %s error frame: %s", mac, frame.strip())
                        statuses = parse_door_statuses(frame)
                        if statuses:
                            self._publish(mac, statuses)
                    if monotonic() - last_ka >= KEEPALIVE_INTERVAL:
                        await conn.request_status()
                        last_ka = monotonic()
            except asyncio.CancelledError:
                await conn.close()
                raise
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("NHK %s connection lost: %s", mac, err)
            finally:
                self._conns.pop(mac, None)
                self._set_available(mac, False)
                await conn.close()
            if not self._stopping:
                await asyncio.sleep(RECONNECT_BACKOFF)
