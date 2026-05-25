import logging
import time
from .Store import Store
from config import Config
import random
from .EvictPool import EvictPool

logger = logging.getLogger(__name__)

class Eviction:
    
    # Triggers the eviction process to free up space
    @staticmethod
    def evict() -> None:
        if Config.EVICTION_STRATEGY == "simple-first":
            Eviction.__evictRandomOne()
        elif Config.EVICTION_STRATEGY == "allkeys-random":
            Eviction.__evictAllkeysRandom()
        elif Config.EVICTION_STRATEGY == "allkeys-lru":
            Eviction.__evictAllKeysLRU()
        else:
            logger.error(f"Invalid eviction strategy: {Config.EVICTION_STRATEGY}")
    
    # Evicts one random key-value pair from the store
    @staticmethod
    def __evictRandomOne():
        if not Store.store:
            return
        # Since python preserve order of insertion in dict
        # retrieval isn't random.
        # So random.choice : to get a random key from keys list
        # and then we can evict it.
        key = random.choice(list(Store.store.keys()))
        Store.delete(key)
    
    # Evicts random (EVICTION_RATIO %) keys from the store
    @staticmethod
    def __evictAllkeysRandom():
        if not Store.store:
            return
        # No of keys tobe evicted
        evictKeys = int(Config.EVICTION_RATIO * float(Config.KEY_LIMIT))
        # Since python preserve order of insertion in dict
        # retrieval isn't random.
        # So random.sample : to get a random keys list from dict keys list
        # and then we can evict it.
        keys = random.sample(list(Store.store.keys()), min(evictKeys, len(Store.store)))
        for key in keys:
            Store.delete(key)
    
    @staticmethod
    def __populateEvictionPool(count: int) -> None:
        if not Store.store:
            return
        # ramdom.sample: reason in comments of "__evictAllKeysRandom" function
        # min(): For getting Keys without raising ValueError
        keys = random.sample(list(Store.store.keys()), min(count, len(Store.store)))
        for key in keys:
            obj = Store.get(key)
            if obj is not None:
                # Add key and its last accessed timestamp (LAT) to the eviction pool
                EvictPool.add(key, obj.getLAT())
    
    @staticmethod
    def __evictAllKeysLRU() -> None:
        evictKeys = int(Config.EVICTION_RATIO * float(Config.KEY_LIMIT))
        i = 0
        while i < evictKeys:
            # If the eviction pool is empty, attempt to populate it with a new sample
            if EvictPool.isEmpty():
                Eviction.__populateEvictionPool(Config.EVICTION_SAMPLE_SIZE)

                # If the store is empty or no candidates are left to populate, stop eviction
                if EvictPool.isEmpty():
                    break

            item = EvictPool.pop()
            if item is None:
                continue
            
            # Retrieve object from store to check for stale/already deleted pool entries
            obj = Store.get(item.key)

            # Skip if the key is stale (already deleted or expired)
            if obj is None:
                continue

            # Delete the key and increment the count of successfully evicted keys
            Store.delete(item.key)
            i+=1
        logger.debug(f"Evicted {i} keys using allkeys-lru eviction strategy")
        return
            