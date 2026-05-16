from typing import Dict
from config import Config
from .redisObject import RedisObject

# In-memory dictionary-based storage with eviction and lazy-deletion support
class Store:
    # Core data structure for storing all keys and their corresponding RedisObjects
    store: Dict[str, RedisObject] = dict()

    # Stores a key-value pair; triggers eviction if the storage limit is exceeded
    @classmethod
    def put(cls, key: str, value: RedisObject) -> None:
        if (len(cls.store) >= Config.KEY_LIMIT):
            from .eviction import Eviction
            Eviction.evict()
        cls.store[key] = value

    # Retrieves an object; deletes and returns None if it has expired
    @classmethod
    def get(cls, key: str) -> RedisObject | None:
        val = cls.store.get(key, None)
        if val is None:
            return None
        if val.isExpired():
            cls.store.pop(key)
            return None
        return val

    # Deletes a key from the store
    @classmethod
    def delete(cls, key: str) -> bool:
        if key in cls.store:
            cls.store.pop(key)
            return True
        return False