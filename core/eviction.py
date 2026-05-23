import logging
from .Store import Store
from config import Config
import random

logger = logging.getLogger(__name__)

class Eviction:
    
    # Triggers the eviction process to free up space
    @staticmethod
    def evict() -> None:
        if Config.EVICTION_STRATEGY == "simple-first":
            Eviction.__evictRandomOne()
        elif Config.EVICTION_STRATEGY == "allkeys-random":
            Eviction.__evictAllkeysRandom()
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
        keys = random.sample(list(Store.store.keys()), evictKeys)
        for key in keys:
            Store.delete(key)
    
    