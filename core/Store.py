import time
from typing import List, Union
from config import Config
from .RedisObject import RedisObject
from .internals.Malloc_internal import MemTracker
from .internals.HashMap import HashMap

class StoreMeta(type):
    @property
    def store(cls) -> HashMap:
        return cls.stores[0]

    @property
    def expires(cls) -> HashMap:
        return cls.expires_list[0]

# In-memory storage with eviction and lazy-deletion support, using HashMap on the C heap
class Store(metaclass=StoreMeta):
    # Core data structures per database using HashMap on the C heap
    stores: List[HashMap] = [HashMap("string", "int64") for _ in range(Config.DB_COUNT)]
    expires_list: List[HashMap] = [HashMap("int64", "int64") for _ in range(Config.DB_COUNT)]

    # Stores a key-value pair; triggers eviction if the storage limit is exceeded
    @classmethod
    def put(cls, key: str, value: RedisObject, ex_duration_sec: int, db: int = 0) -> None:
        # Trigger eviction while total allocated C-memory exceeds MEMORY_LIMIT and keys exist to evict
        while len(cls.stores[db]) > 0 and MemTracker.allocated > Config.MEMORY_LIMIT:
            from .eviction import Eviction
            Eviction.evict(db)

        # Transfer ownership to the store and get the raw struct pointer address
        ptr = value.release()
        cls.stores[db].set(key, ptr)
        
        if ex_duration_sec > 0:
            cls.setExpiry(ptr, ex_duration_sec, db)

        # After inserting, if we exceed the limit, evict immediately to maintain the invariant
        while len(cls.stores[db]) > 0 and MemTracker.allocated > Config.MEMORY_LIMIT:
            from .eviction import Eviction
            Eviction.evict(db)

    # Retrieves an object; deletes and returns None if it has expired
    @classmethod
    def get(cls, key: str, db: int = 0) -> RedisObject | None:
        ptr = cls.stores[db].get(key, None)
        if ptr is None or ptr == 0:
            return None
        
        if cls.hasExpired(ptr, db):
            cls.delete(key, db)
            return None
            
        # Return a temporary RedisObject wrapper
        val = RedisObject(ptr=ptr)
        val.updateLAT()
        return val

    # Deletes a key from the store
    @classmethod
    def delete(cls, key: str, db: int = 0) -> bool:
        if key not in cls.stores[db]:
            return False
        
        ptr = cls.stores[db].get(key)
        cls.stores[db].delete(key)
        if ptr is not None and ptr != 0:
            cls.expires_list[db].delete(ptr)

            # Reconstruct temporary owner to free underlying resources
            val = RedisObject(ptr=ptr)
            val.val = None  # Free nested C structure
            val.free()      # Free RedisObjectStruct itself
        
        return True
    
    @classmethod
    def setExpiry(cls, obj: Union[RedisObject, int], ex_duration_sec: int, db: int = 0) -> None:
        ptr = obj if isinstance(obj, int) else obj.release()
        cls.expires_list[db].set(ptr, int(time.time()) + int(ex_duration_sec))
    
    @classmethod
    def hasExpired(cls, obj: Union[RedisObject, int], db: int = 0) -> bool:
        ptr = obj if isinstance(obj, int) else obj._struct_ptr.ptr
        expiry = cls.expires_list[db].get(ptr, None)
        if expiry is None:
            return False
        return expiry <= int(time.time())
    
    @classmethod
    def getExpiry(cls, obj: Union[RedisObject, int], db: int = 0) -> int:
        ptr = obj if isinstance(obj, int) else obj._struct_ptr.ptr
        expiry = cls.expires_list[db].get(ptr, None)
        if expiry is None:
            return -1
        return expiry
