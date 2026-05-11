import logging
from .store import Store
from config import Config

logger = logging.getLogger(__name__)

class Eviction:
    
    # Triggers the eviction process to free up space
    @staticmethod
    def evict() -> None:
        if Config.EVICTION_STRATEGY == "simple-first":
            Eviction.__evictFirst()
        else:
            logger.error(f"Invalid eviction strategy: {Config.EVICTION_STRATEGY}")
    
    # Evicts the first key-value pair from the store
    @staticmethod
    def __evictFirst():
        for key in Store.store.keys():
            Store.delete(key)
            return