from typing import Dict

from infuse.breaker.constants import STATE_CLOSED

INFUSE_ENABLED = True

INFUSE_CACHES: Dict[str, dict] = {
    "infuse": {"HOST": "localhost", "PORT": 6379, "DATABASE": 15}
}

INFUSE_RESET_TIMEOUT = 15
INFUSE_MAX_FAILURE = 5

INFUSE_INITIAL_STATE = STATE_CLOSED
INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE = "{env}:{service_name}"
