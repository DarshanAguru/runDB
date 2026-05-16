from typing import Any
from .redisObject import REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS

class Encoder:
    # Encodes data into RESP format based on type and flags
    @staticmethod
    def encode(val: Any, bulk: bool = False) -> bytes:
        if isinstance(val, int):
            return f":{val}\r\n".encode("utf-8")
        
        if isinstance(val, list):
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