"""
Constants for the Nice Garage (MyNice) integration.

These mirror the values reverse-engineered from the MyNice app.
"""
from __future__ import annotations

from homeassistant.const import Platform

DOMAIN = "mynice"
PLATFORMS = [Platform.COVER]

# --- Cloud REST (OAuth2) ---------------------------------------------------- #
BASE_URL = "https://integration.niceappdomain.com/myNiceCloud/"
CLIENT_ID = "android-client-id"
CLIENT_SECRET = "android-client-id_21"

# Headers the app always sends (cosmetic, but mirror the app).
COMMON_HEADERS = {
    "OS": "Android",
    "OSVersion": "13",
    "DeviceModel": "Google Pixel pixel",
    "Accept-Language": "en",
    "Accept-Encoding": "gzip, deflate",
}

# --- NHK device socket ------------------------------------------------------ #
PROXY_HOST = "integration.niceappdomain.com"
PROXY_PORT = 7890
STX = 0x02
ETX = 0x03

# --- Config entry keys ------------------------------------------------------ #
CONF_TOKEN = "token"  # cached OAuth token dict (access/refresh/_expires_at)

# --- Door state vocabulary (DoorStatus values) ------------------------------ #
STATE_OPEN = "open"
STATE_CLOSED = "closed"
STATE_OPENING = "opening"
STATE_CLOSING = "closing"
STATE_STOPPED = "stopped"
MOVING_STATES = {STATE_OPENING, STATE_CLOSING, "stopping"}
SETTLED_STATES = {STATE_OPEN, STATE_CLOSED, STATE_STOPPED}

# Dispatcher signals to cover entities (formatted with the automation id).
SIGNAL_STATE = f"{DOMAIN}_state_{{}}"
SIGNAL_AVAILABLE = f"{DOMAIN}_avail_{{}}"

# Timings.
KEEPALIVE_INTERVAL = 25.0  # seconds; proxy drops idle sockets
RECONNECT_BACKOFF = 5.0  # seconds between socket reconnect attempts
DEVICE_CACHE_TTL = 6 * 3600  # device structure rarely changes
