"""Config flow for the Nice Garage (MyNice) integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import NiceApiError, NiceAuthError, NiceCloud
from .const import CONF_TOKEN, DOMAIN

_LOGGER = logging.getLogger(__name__)

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
    }
)


async def _validate(hass, username: str, password: str) -> dict:
    """Log in and discover; return entry data or raise for the error type."""
    session = async_get_clientsession(hass)
    cloud = NiceCloud(session, username=username, password=password)
    await cloud.async_login()  # raises NiceAuthError on bad credentials
    doors = await cloud.async_discover()  # raises NiceApiError on network issues
    if not doors:
        raise NoDoors
    return {
        CONF_USERNAME: username,
        CONF_PASSWORD: password,
        CONF_TOKEN: cloud.token,
    }


class NoDoors(Exception):
    """Account has no garage doors."""


class NiceGarageConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Nice Garage."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            username = user_input[CONF_USERNAME].strip()
            await self.async_set_unique_id(username.lower())
            self._abort_if_unique_id_configured()
            try:
                data = await _validate(self.hass, username, user_input[CONF_PASSWORD])
            except NiceAuthError:
                errors["base"] = "invalid_auth"
            except NoDoors:
                errors["base"] = "no_doors"
            except NiceApiError:
                errors["base"] = "cannot_connect"
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Unexpected error validating Nice account")
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(title=username, data=data)

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_SCHEMA, errors=errors
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Handle re-authentication when stored credentials stop working."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        assert reauth_entry is not None

        if user_input is not None:
            username = reauth_entry.data[CONF_USERNAME]
            try:
                data = await _validate(self.hass, username, user_input[CONF_PASSWORD])
            except NiceAuthError:
                errors["base"] = "invalid_auth"
            except NoDoors:
                errors["base"] = "no_doors"
            except NiceApiError:
                errors["base"] = "cannot_connect"
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Unexpected error during reauth")
                errors["base"] = "unknown"
            else:
                self.hass.config_entries.async_update_entry(reauth_entry, data=data)
                await self.hass.config_entries.async_reload(reauth_entry.entry_id)
                return self.async_abort(reason="reauth_successful")

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema({vol.Required(CONF_PASSWORD): str}),
            description_placeholders={"username": reauth_entry.data[CONF_USERNAME]},
            errors=errors,
        )
