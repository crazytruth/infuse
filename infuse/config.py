from typing import Dict, List

from pybreaker import STATE_CLOSED


#: The cache for where the states will be saved.
INFUSE_CACHES: Dict[str, dict] = {
    "infuse": {"HOST": "localhost", "PORT": 6379, "DATABASE": 15}
}

#: The reset timeout in seconds to retry.
INFUSE_BREAKER_RESET_TIMEOUT: int = 15

#: The number of consecutive failures to another application before the circuit breaker trips.
INFUSE_BREAKER_MAX_FAILURE: int = 5

#: The initial state when a new instance of this application is launched.
INFUSE_INITIAL_CIRCUIT_STATE: str = STATE_CLOSED

#: The fallback state when state is unable to be retrieved from storage.
INFUSE_FALLBACK_CIRCUIT_STATE: str = STATE_CLOSED

#: The paths of the listeners you would like to initialize the breaker with.
INFUSE_BREAKER_LISTENERS: List[str] = []

#: The paths of the exceptions to exclude from opening the circuit.
INFUSE_BREAKER_EXCLUDE_EXCEPTIONS: List[str] = [
    "insanic.services.adapters.HTTPStatusError"
]

#: The default redis key template.
INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE: str = "{env}:{service_name}"
