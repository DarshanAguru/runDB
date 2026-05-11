from .store import Store

class Expiration:
    
    # Samples a subset of keys and deletes those that have expired
    @staticmethod
    def expireSamples() -> float:
        limit = 20
        expire_count = 0

        for k, v in Store.store.items():
            if v.getExpiresAt() != -1:
                limit -= 1
                if v.isExpired():
                    Store.delete(k)
                    expire_count += 1
            if limit == 0:
                break
        
        return expire_count / 20.0