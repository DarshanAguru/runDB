import logging
from .store import Store
from config import Config
import random

logger = logging.getLogger(__name__)

class Eviction:
    
    # Triggers the eviction process to free up space
    @staticmethod
    def evict() -> None:
        if Config.EVICTION_STRATEGY == "simple-first":
            Eviction.__evictFirst()
        elif Config.EVICTION_STRATEGY == "allkeys-random":
            Eviction.__evictAllkeysRandom()
        else:
            logger.error(f"Invalid eviction strategy: {Config.EVICTION_STRATEGY}")
    
    # Evicts the first key-value pair from the store
    @staticmethod
    def __evictFirst():
        keys = list(Store.store.keys())
        # random.shuffle because python doesnt randomise the dict key retrieval
        # to make it truely random we need to shuffle the keys in random order 
        # and then perform deletion
        random.shuffle(keys)
        for key in keys:
            Store.delete(key)
            break
    
    # Evicts random (EVICTION_RATIO %) keys from the store
    @staticmethod
    def __evictAllkeysRandom():
        # No of keys tobe evicted
        evictKeys = int(Config.EVICTION_RATIO * float(Config.KEY_LIMIT))
        keys = list(Store.store.keys())
        # random.shuffle -- explaination given at line 23 in "__evictFirst()" method :)
        random.shuffle(keys)
        for key in keys:
            Store.delete(key)
            evictKeys -= 1
            if evictKeys <= 0:
                break
    
    