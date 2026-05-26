import logging
import random
from .Store import Store

logger = logging.getLogger(__name__)

class Expiration:
    
    # Samples a subset of keys and deletes those that have expired
    @staticmethod
    def __expireSamples() -> float:
        limit = 20
        expire_count = 0
        
        if not Store.store:
            return 0.0

        # Sample keys first to avoid RuntimeError: dictionary changed size during iteration
        sampled_keys = random.sample(list(Store.store.keys()), min(limit, len(Store.store)))
        for k in sampled_keys:
            v = Store.store.get(k)
            if v is None:
                continue
            if Store.hasExpired(v):
                Store.delete(k)
                expire_count += 1

        return expire_count / len(sampled_keys)
    
    # Periodically samples and deletes expired keys from the store
    @staticmethod
    def deleteExpiredKeys() -> None:
        frac = Expiration.__expireSamples()
        while frac > 0.25:
            frac = Expiration.__expireSamples()
        
        # Logging the number of deleted keys only if keys are deleted
        # just for logging, no use for server logic, can be commented out.
        val = int(frac * min(20, len(Store.store))) 
        if val > 0:
            logger.debug(f"Deleted {val} the expired but undeleted keys.")