import logging
import time
from .Store import Store
from config import Config
import random
from .EvictPool import EvictPool

logger = logging.getLogger(__name__)

class Eviction:
    
    # Triggers the eviction process to free up space in a specific database
    @staticmethod
    def evict(db: int = 0) -> None:
        if Config.EVICTION_STRATEGY == "simple-first":
            Eviction.__evictRandomOne(db)
        elif Config.EVICTION_STRATEGY == "allkeys-random":
            Eviction.__evictAllkeysRandom(db)
        elif Config.EVICTION_STRATEGY == "allkeys-lru":
            Eviction.__evictAllKeysLRU(db)
        else:
            logger.error(f"Invalid eviction strategy: {Config.EVICTION_STRATEGY}")
    
    # Evicts one random key-value pair from the store in a database
    @staticmethod
    def __evictRandomOne(db: int):
        store = Store.stores[db]
        if not store:
            return
        key = random.choice(list(store.keys()))
        Store.delete(key, db)
    
    # Evicts random (EVICTION_RATIO %) keys from the store in a database
    @staticmethod
    def __evictAllkeysRandom(db: int):
        store = Store.stores[db]
        if not store:
            return
        evictKeys = max(1, int(Config.EVICTION_RATIO * len(store)))
        keys = random.sample(list(store.keys()), min(evictKeys, len(store)))
        for key in keys:
            Store.delete(key, db)
    
    @staticmethod
    def __populateEvictionPool(db: int, count: int) -> None:
        store = Store.stores[db]
        if not store:
            return
        keys = random.sample(list(store.keys()), min(count, len(store)))
        for key in keys:
            obj = store.get(key)
            if obj is not None:
                # Add key and its last accessed timestamp (LAT) to the eviction pool
                EvictPool.add(key, obj.getLAT())
    
    @staticmethod
    def __evictAllKeysLRU(db: int) -> None:
        store = Store.stores[db]
        # Number of keys to be evicted is a percentage of current active keys (minimum 1 key)
        evictKeys = max(1, int(Config.EVICTION_RATIO * len(store)))
        i = 0
        while i < evictKeys:
            # If the eviction pool is empty, attempt to populate it with a new sample
            if EvictPool.isEmpty():
                Eviction.__populateEvictionPool(db, Config.EVICTION_SAMPLE_SIZE)

                # If the store is empty or no candidates are left to populate, stop eviction
                if EvictPool.isEmpty():
                    break

            item = EvictPool.pop()
            if item is None:
                continue
            
            # Retrieve object from store to check for stale/already deleted pool entries
            obj = store.get(item.key)

            # Skip if the key is stale (already deleted or expired)
            if obj is None:
                continue

            # Delete the key and increment the count of successfully evicted keys
            Store.delete(item.key, db)
            i+=1
        logger.debug(f"Evicted {i} keys from db{db} using allkeys-lru eviction strategy")
        return
            