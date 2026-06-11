from typing import List, Dict
from config import Config

# Track and manage keyspace and memory statistics across multiple Redis databases
class Stats:    

    # Retrieves the memory usage statistics in bytes
    @classmethod
    def getMemoryStats(cls) -> Dict[str, int]:
        from .internals.Malloc_internal import MallocInternal
        used = MallocInternal.zmalloc_used_memory()
        return {
            "used_memory": used,
            "max_memory": Config.MEMORY_LIMIT,
            "available_memory": Config.MEMORY_LIMIT - used
        }

    # Retrieves the keyspace statistics, including keys count, expires count, and average TTL in milliseconds
    @classmethod
    def get_keyspace_stats(cls) -> List[Dict[str, int]]:
        from .Store import Store
        import time

        stats_list = []
        current_time = int(time.time())

        for db_idx in range(Config.DB_COUNT):
            keys = len(Store.stores[db_idx])
            expires = len(Store.expires_list[db_idx])
            if expires > 0:
                total_ttl_ms = 0
                for obj, expiry in Store.expires_list[db_idx].items():
                    ttl_sec = max(0, expiry - current_time)
                    total_ttl_ms += ttl_sec * 1000
                avg_ttl = int(total_ttl_ms / expires)
            else:
                avg_ttl = 0

            stats_list.append({
                "keys": keys,
                "expires": expires,
                "avg_ttl": avg_ttl
            })
        return stats_list