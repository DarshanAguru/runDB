from typing import Dict
from config import Config
from .RedisObject import RedisObject
from .Stats import Stats

# In-memory dictionary-based storage with eviction and lazy-deletion support
class Store:
    # Core data structure for storing all keys and their corresponding RedisObjects
    store: Dict[str, RedisObject] = dict()

    # Stores a key-value pair; triggers eviction if the storage limit is exceeded
    # also increments keys count in Stats
    @classmethod
    def put(cls, key: str, value: RedisObject) -> None:
        if (len(cls.store) >= Config.KEY_LIMIT):
            from .eviction import Eviction
            Eviction.evict()
        cls.store[key] = value
        Stats.increment(0, "keys")

    # Retrieves an object; deletes and returns None if it has expired
    @classmethod
    def get(cls, key: str) -> RedisObject | None:
        val = cls.store.get(key, None)
        if val is None:
            return None
        if val.isExpired():
            cls.delete(key)
            return None
        val.updateLAT()
        return val

    # Deletes a key from the store also decrements keys count in Stats.
    @classmethod
    def delete(cls, key: str) -> bool:
        if key in cls.store:
            cls.store.pop(key)
            Stats.decrement(0, "keys")
            return True
        return False