from typing import Dict

from pybreaker import STATE_CLOSED

#: Flag for whether or not to enable circuit breaking throughout the application.
INFUSE_ENABLED: bool = True

#: The cache for where the states will be saved.
INFUSE_CACHES: Dict[str, dict] = {
    "infuse": {"HOST": "localhost", "PORT": 6379, "DATABASE": 15}
}

#: The reset timeout in seconds to retry.
INFUSE_RESET_TIMEOUT: int = 15

#: The number of consecutive failures to another application before the circuit breaker trips.
INFUSE_MAX_FAILURE: int = 5

#: The initial state when a new instance of this application is launched.
INFUSE_INITIAL_STATE: str = STATE_CLOSED

#: The default redis key template.
INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE: str = "{env}:{service_name}"
