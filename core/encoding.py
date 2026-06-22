from typing import Any
from .RedisObject import REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS

# Encoder handles data serialization and type deduction:
# - RESP Serialization: Encodes integers, arrays, simple strings, and bulk strings into standard RESP bytes.
# - Type Deduction: Inspects values to identify optimal Redis Object types and encodings (e.g. using
#   INT for parseable numbers, EMBSTR for strings <= 44 bytes, or RAW for longer strings).
class Encoder:
    # Encodes data into RESP format based on type and flags
    @staticmethod
    def encode(val: Any, bulk: bool = False) -> bytes:
        if val is None:
            return b"$-1\r\n"
            
        if isinstance(val, int):
            return f":{val}\r\n".encode("utf-8")
        
        if isinstance(val, list):
            if len(val) == 0:
                return b"+(empty array)\r\n"
            res = b"*" + str(len(val)).encode("utf-8") + b"\r\n"
            for item in val:
                # Array elements are encoded as bulk strings by default for commands
                res += Encoder.encode(item, bulk=True)
            return res
        
        if bulk:
            return f"${len(val)}\r\n{val}\r\n".encode("utf-8")
        else:
            return f"+{val}\r\n".encode("utf-8")

    # Deduces the Redis type and encoding for a given string value
    @staticmethod
    def deduceTypeEncoding(value: str) -> tuple[int, int]:
        try:
            int(value)
            return REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.INT
        except (ValueError, TypeError):
            # If string length is <= 44, use EMBSTR (embedded string) encoding
            if len(value) <= 44:
                return REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR
        return REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.RAW