from typing import Any, Dict
import time
from config import Config


class Value:
    def __init__(self, val: Any, ex_duration_ms: int) -> None:
        self.val = val
        self.expire_at = -1
        if ex_duration_ms > 0:
            self.expire_at = time.time() * 1000 + ex_duration_ms
    
    def getExpiresAt(self) -> int:
        return self.expire_at

    def setExpiresAt(self, ex_duration_ms: int) -> None:
        self.expire_at = time.time() * 1000 + ex_duration_ms
    
    def getValue(self) -> Any:
        return self.val
    
    def isExpired(self) -> bool:
        if self.expire_at == -1:
            return False    
        return self.expire_at <= time.time() * 1000





class Store:
    store: Dict[str, Value] = dict()

    @classmethod
    def put(cls, key: str, value: Value) -> None:
        if (len(cls.store) >= Config.KEY_LIMIT):
            from .eviction import Eviction
            Eviction.evict()
        cls.store[key] = value

    @classmethod
    def get(cls, key: str) -> Value | None:
        val = cls.store.get(key, None)
        if val is None:
            return None
        if val.isExpired():
            cls.store.pop(key)
            return None
        return val

    @classmethod
    def delete(cls, key: str) -> bool:
        if key in cls.store:
            cls.store.pop(key)
            return True
        return False