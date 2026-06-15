import logging
import random
from .Store import Store

logger = logging.getLogger(__name__)

# Expiration implements the active key expiration daemon loop:
# - Active Sampling: Periodically samples a random subset (20 keys) to check for expirations.
# - Probabilistic Threshold: If > 25% of sampled keys are expired, it immediately resamples and 
#   expires again in a loop, ensuring expired keys do not take up too much memory.
# - Lazy Expiry fallback: Expired keys are also evicted when accessed in Store.get().
class Expiration:
    
    # Samples a subset of keys in a database and deletes those that have expired
    @staticmethod
    def __expireSamples(db: int) -> float:
        limit = 20
        expire_count = 0
        store = Store.stores[db]
        
        if not store:
            return 0.0

        # Sample keys first to avoid RuntimeError: dictionary changed size during iteration
        sampled_keys = random.sample(list(store.keys()), min(limit, len(store)))
        for k in sampled_keys:
            v = store.get(k)
            if v is None:
                continue
            if Store.hasExpired(v, db):
                Store.delete(k, db)
                expire_count += 1

        return expire_count / len(sampled_keys)
    
    # Periodically samples and deletes expired keys from all stores
    @staticmethod
    def deleteExpiredKeys() -> None:
        for db in range(len(Store.stores)):
            if not Store.stores[db]:
                continue
            frac = Expiration.__expireSamples(db)
            while frac > 0.25:
                frac = Expiration.__expireSamples(db)
            
            val = int(frac * min(20, len(Store.stores[db]))) 
            if val > 0:
                logger.debug(f"Deleted {val} expired keys from db{db}.")