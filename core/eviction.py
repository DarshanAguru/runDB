from .store import Store


class Eviction:
    
    # Triggers the eviction process to free up space
    @staticmethod
    def evict() -> None:
        Eviction.__evictFirst()
    
    # Evicts the first key-value pair from the store
    @staticmethod
    def __evictFirst():
        for key in Store.store.keys():
            Store.delete(key)
            return