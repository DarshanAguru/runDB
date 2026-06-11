import time
from typing import Dict, List
from config import Config
from .RedisObject import RedisObject
from .internals.Malloc_internal import MemTracker

class StoreMeta(type):
    @property
    def store(cls) -> Dict[str, RedisObject]:
        return cls.stores[0]

    @property
    def expires(cls) -> Dict[RedisObject, int]:
        return cls.expires_list[0]

# In-memory dictionary-based storage with eviction and lazy-deletion support
class Store(metaclass=StoreMeta):
    # Core data structures per database
    stores: List[Dict[str, RedisObject]] = [dict() for _ in range(Config.DB_COUNT)]
    expires_list: List[Dict[RedisObject, int]] = [dict() for _ in range(Config.DB_COUNT)]

    # Stores a key-value pair; triggers eviction if the storage limit is exceeded
    @classmethod
    def put(cls, key: str, value: RedisObject, ex_duration_sec: int, db: int = 0) -> None:
        # Trigger eviction while total allocated C-memory exceeds MEMORY_LIMIT and keys exist to evict
        while len(cls.stores[db]) > 0 and MemTracker.allocated > Config.MEMORY_LIMIT:
            from .eviction import Eviction
            Eviction.evict(db)

        cls.stores[db][key] = value
        if ex_duration_sec > 0:
            cls.setExpiry(value, ex_duration_sec, db)

        # After inserting, if we exceed the limit, evict immediately to maintain the invariant
        while len(cls.stores[db]) > 0 and MemTracker.allocated > Config.MEMORY_LIMIT:
            from .eviction import Eviction
            Eviction.evict(db)

    # Retrieves an object; deletes and returns None if it has expired
    @classmethod
    def get(cls, key: str, db: int = 0) -> RedisObject | None:
        val = cls.stores[db].get(key, None)
        if val is None:
            return None
        if cls.hasExpired(val, db):
            cls.delete(key, db)
            return None
        val.updateLAT()
        return val

    # Deletes a key from the store
    @classmethod
    def delete(cls, key: str, db: int = 0) -> bool:
        if key not in cls.stores[db]:
            return False
        
        val = cls.stores[db].pop(key)
        if val in cls.expires_list[db]:
            cls.expires_list[db].pop(val)

        # Explicitly free the C-allocated memory of the value immediately
        if hasattr(val, "_val") and val._val is not None:
            val.val = None  # Setting to None triggers the property setter which calls free()
        
        return True
    
    @classmethod
    def setExpiry(cls, obj: RedisObject, ex_duration_sec: int, db: int = 0) -> None:
        cls.expires_list[db][obj] = int(time.time()) + int(ex_duration_sec)
    
    @classmethod
    def hasExpired(cls, obj: RedisObject, db: int = 0) -> bool:
        return cls.expires_list[db].get(obj, float('inf')) <= int(time.time())
    
    @classmethod
    def getExpiry(cls, obj: RedisObject, db: int = 0) -> int:
        return cls.expires_list[db].get(obj, -1)
