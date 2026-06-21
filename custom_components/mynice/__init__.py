"""The Nice Garage (MyNice) integration."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import NiceApiError, NiceAuthError, NiceCloud
from .const import CONF_TOKEN, DOMAIN, PLATFORMS
from .coordinator import NiceHub

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Nice Garage from a config entry."""
    session = async_get_clientsession(hass)

    def _save_token(token: dict) -> None:
        # Persist the refreshed token into the entry (HA-native credential cache).
        hass.config_entries.async_update_entry(
            entry, data={**entry.data, CONF_TOKEN: token}
        )

    cloud = NiceCloud(
        session,
        username=entry.data.get(CONF_USERNAME),
        password=entry.data.get(CONF_PASSWORD),
        token=entry.data.get(CONF_TOKEN),
        on_token=_save_token,
    )

    try:
        doors = await cloud.async_discover()
    except NiceAuthError as err:
        raise ConfigEntryAuthFailed(str(err)) from err
    except NiceApiError as err:
        raise ConfigEntryNotReady(str(err)) from err

    hub = NiceHub(hass, entry, cloud, doors)
    await hub.async_start()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = hub
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    hub: NiceHub | None = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if hub is not None:
        await hub.async_stop()
    return unload_ok
