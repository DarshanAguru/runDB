from typing import Any
import time
from config import Config

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
    __slots__ = ["val", "expire_at", "typeEncoding", "lat"]

    def __init__(self, val: Any, o_type: int, o_encoding: int) -> None:
        self.val = val
        # Approximation LRU with last seen 24 bits from time in seconds
        # Storing Last Accessed at Time (LAT)
        self.lat = self.getLRUClock()
        
        # Pack type (high 4 bits) and encoding (low 4 bits) into a single byte
        self.typeEncoding = ((o_type & 0x0F) << 4) | (o_encoding & 0x0F)

    
    def updateLAT(self) -> None:
        self.lat = self.getLRUClock()
    
    def getLAT(self) -> int:
        return self.lat
    
    def getType(self) -> int:
        return (self.typeEncoding >> 4) & 0x0F
    
    def getEncoding(self) -> int:
        return self.typeEncoding & 0x0F
    
    def getValue(self) -> Any:
        return self.val
    
    def getLRUClock(self) -> int:
        return int(time.time()) & Config.LRU_BITS_MASK
