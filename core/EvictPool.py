import bisect
from typing import Dict, List
from config import Config
import time

class PoolItem:
    def __init__(self, key: str, lat: int):
        self.key = key
        self.lat = lat
    
class EvictPool:
    # LRU Eviction Pool
    pool: List[PoolItem] = []

    # Map for quick lookup and removal of keys in the eviction pool
    keySet: Dict[str, PoolItem] = dict()
    
    @classmethod
    def add(cls, key: str, lat: int) -> None:
        if key in cls.keySet:
            return
        
        item = PoolItem(key, lat)
        # If the pool is not yet full, simply insert in sorted order and track in keySet
        if len(cls.pool) < Config.EVICTION_POOL_SIZE:
            cls.__insertSorted(item)
            cls.keySet[key] = item
            return
        
        # If full, compare idle time with the worst candidate (index 0, smallest idle time).
        # We only keep the new candidate if it is better (has a larger idle time) than our current worst.
        if cls.__getIdleTime(item.lat) > cls.__getIdleTime(cls.pool[0].lat):
            removed = cls.pool.pop(0)  # Evict the worst candidate from pool
            cls.keySet.pop(removed.key, None)
            cls.__insertSorted(item)   # Insert new better candidate in sorted order
            cls.keySet[key] = item
    
    @classmethod
    def remove(cls, key: str) -> None:
        item = cls.keySet.pop(key, None)
        if item is None:
            return

        try:
            cls.pool.remove(item)
        except ValueError:
            pass

    @classmethod
    def pop(cls) -> PoolItem | None:
        if len(cls.pool) == 0:
            return None
        item = cls.pool.pop(-1)  # The last element has the maximum idle time
        cls.keySet.pop(item.key, None)
        return item
    
    @classmethod
    def isEmpty(cls) -> bool:
        return len(cls.pool) == 0

    @classmethod
    def __getLRUClock(cls) -> int:
        return int(time.time()) & Config.LRU_BITS_MASK
    
    @classmethod
    def __getIdleTime(cls, lat: int) -> int:
        curr = cls.__getLRUClock()
        if (curr >= lat):
            return curr - lat
        return (Config.LRU_BITS_MASK - lat) + curr
    
    @classmethod
    def __insertSorted(cls, item: PoolItem) -> None:
        idleTime = cls.__getIdleTime(item.lat)
        # insert in the correct position based on the idle time.
        # still takes O(n) but better than O(n log n) for repeated sorting.
        bisect.insort(cls.pool, item, key=lambda x: cls.__getIdleTime(x.lat))