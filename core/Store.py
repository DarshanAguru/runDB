import time
from typing import Dict
from config import Config
from .RedisObject import RedisObject
from .Stats import Stats

# In-memory dictionary-based storage with eviction and lazy-deletion support
class Store:
    # Core data structure for storing all keys and their corresponding RedisObjects
    store: Dict[str, RedisObject] = dict()

    # To store the keys with expiry set (Used for active eviction)
    expires: Dict[RedisObject, int] = dict()

    # Stores a key-value pair; triggers eviction if the storage limit is exceeded
    # also increments keys count in Stats
    @classmethod
    def put(cls, key: str, value: RedisObject, ex_duration_sec: int) -> None:
        if (len(cls.store) >= Config.KEY_LIMIT):
            from .eviction import Eviction
            Eviction.evict()
        cls.store[key] = value
        if ex_duration_sec > 0:
            cls.setExpiry(value, ex_duration_sec)
        Stats.increment(0, "keys")

    # Retrieves an object; deletes and returns None if it has expired
    @classmethod
    def get(cls, key: str) -> RedisObject | None:
        val = cls.store.get(key, None)
        if val is None:
            return None
        if cls.hasExpired(val):
            cls.delete(key)
            return None
        val.updateLAT()
        return val

    # Deletes a key from the store also decrements keys count in Stats.
    @classmethod
    def delete(cls, key: str) -> bool:
        if key not in cls.store:
            return False
        
        val = cls.store.pop(key)
        if val in cls.expires:
            cls.expires.pop(val)
        Stats.decrement(0, "keys")
        
        return True
    
    @classmethod
    def setExpiry(cls, obj: RedisObject,  ex_duration_sec: int) -> None:
        cls.expires[obj] = int(time.time()) + int(ex_duration_sec)
    
    @classmethod
    def hasExpired(cls, obj: RedisObject) -> bool:
        return cls.expires.get(obj, float('inf')) <= int(time.time())
    
    @classmethod
    def getExpiry(cls, obj: RedisObject) -> int:
        return cls.expires.get(obj, -1)
        
