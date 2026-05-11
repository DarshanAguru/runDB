from .store import Store


class Eviction:
    
    @staticmethod
    def evict() -> None:
        Eviction.__evictFirst()
    
    @staticmethod
    def __evictFirst():
        for key in Store.store.keys():
            Store.delete(key)
            return