from typing import Any
import time

# Redis object type definitions
class REDIS_OBJECT_TYPES:
    TYPE_STRING: int = 0
    
# Redis object encoding definitions
class REDIS_OBJECT_ENCODINGS:
    RAW: int = 0
    INT: int = 1
    EMBSTR: int = 8

# Represents a stored Redis value with metadata (expiration, type, encoding)
class RedisObject:
    # Use slots to minimize memory overhead per object
    __slots__ = ["val", "expire_at", "typeEncoding"]

    def __init__(self, val: Any, ex_duration_ms: int, o_type: int, o_encoding: int) -> None:
        self.val = val
        self.expire_at = -1
        # Pack type (high 4 bits) and encoding (low 4 bits) into a single byte
        self.typeEncoding = ((o_type & 0x0F) << 4) | (o_encoding & 0x0F)
        if ex_duration_ms > 0:
            self.expire_at = int(time.time() * 1000) + ex_duration_ms
    
    def getType(self) -> int:
        return (self.typeEncoding >> 4) & 0x0F
    
    def getEncoding(self) -> int:
        return self.typeEncoding & 0x0F

    def getExpiresAt(self) -> int:
        return self.expire_at

    def setExpiresAt(self, ex_duration_ms: int) -> None:
        self.expire_at = int(time.time() * 1000) + ex_duration_ms
    
    def getValue(self) -> Any:
        return self.val
    
    # Returns True if the object has expired based on the current time
    def isExpired(self) -> bool:
        if self.expire_at == -1:
            return False    
        return self.expire_at <= time.time() * 1000
