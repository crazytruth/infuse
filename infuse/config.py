INFUSE_CACHE = {
    "infuse": {
        "ENGINE": "aioredis",
        "CONNECTION_INTERFACE": "create_redis_pool",
        "CLOSE_CONNECTION_INTERFACE": (('close',), ("wait_closed",)),
        "DATABASE": 3
    }
}

INFUSE_RESET_TIMEOUT = 15
INFUSE_MAX_FAILURE = 5
