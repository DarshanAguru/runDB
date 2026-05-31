from typing import Any
import time
import ctypes
import weakref
from config import Config
from .internals.Malloc import Malloc
from .internals.Malloc_internal import MallocInternal

# Redis object type definitions
class REDIS_OBJECT_TYPES:
    TYPE_STRING: int = 0
    
# Redis object encoding definitions
class REDIS_OBJECT_ENCODINGS:
    RAW: int = 0
    INT: int = 1
    EMBSTR: int = 8


class RedisObjectStruct(ctypes.Structure):
    _fields_ = [
        ("typeEncoding", ctypes.c_uint8),
        ("lat", ctypes.c_uint32),
        ("ptr", ctypes.c_void_p),
        ("size", ctypes.c_size_t)
    ]


# Represents a stored Redis value with metadata (expiration, type, encoding)
class RedisObject:
    # Use slots to minimize memory overhead per object, supporting weak references for finalizers
    __slots__ = ["_struct_ptr", "_finalizer", "__weakref__"]

    def __init__(self, val: Any, o_type: int, o_encoding: int) -> None:
        # 1. Allocate RedisObjectStruct itself in native C memory
        self._struct_ptr = Malloc.alloc_struct(RedisObjectStruct)
        
        # 2. Populate standard fields
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        struct.typeEncoding = ((o_type & 0x0F) << 4) | (o_encoding & 0x0F)
        struct.lat = self.getLRUClock()
        struct.ptr = None
        struct.size = 0

        # Register a finalizer to ensure both the nested value pointer and the struct itself are freed
        self._finalizer = weakref.finalize(
            self,
            self._cleanup,
            self._struct_ptr
        )

        # 3. Call property setter to allocate and set the inner value pointer
        self.val = val

    @staticmethod
    def _cleanup(struct_ptr: MallocInternal) -> None:
        from .internals.Malloc_internal import libc, MemTracker
        if struct_ptr and struct_ptr.ptr:
            struct = ctypes.cast(struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
            if struct.ptr:
                libc.free(struct.ptr)
                MemTracker.free(struct.size)
            struct_ptr.free()

    def free(self) -> None:
        if self._finalizer.alive:
            self._finalizer()

    @property
    def val(self) -> Any:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        return struct

    @val.setter
    def val(self, new_val: Any) -> None:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        
        # Free existing data pointer if present
        if struct.ptr:
            from .internals.Malloc_internal import libc, MemTracker
            libc.free(struct.ptr)
            MemTracker.free(struct.size)
            struct.ptr = None
            struct.size = 0

        if new_val is None:
            return

        encoding = self.getEncoding()
        if encoding == REDIS_OBJECT_ENCODINGS.INT:
            size = ctypes.sizeof(ctypes.c_int32)
            from .internals.Malloc_internal import libc, MemTracker
            ptr = libc.malloc(size)
            if not ptr:
                raise MemoryError("MEMORY OVERFLOW")
            MemTracker.alloc(size)
            ctypes.cast(ptr, ctypes.POINTER(ctypes.c_int32))[0] = int(new_val)
            struct.ptr = ptr
            struct.size = size
        else:
            if isinstance(new_val, str):
                data = new_val.encode()
            else:
                data = bytes(new_val)
            size = len(data) + 1
            from .internals.Malloc_internal import libc, MemTracker
            ptr = libc.malloc(size)
            if not ptr:
                raise MemoryError("MEMORY OVERFLOW")
            MemTracker.alloc(size)
            ctypes.memmove(ptr, data + b"\0", size)
            struct.ptr = ptr
            struct.size = size
    
    def updateLAT(self) -> None:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        struct.lat = self.getLRUClock()
    
    def getLAT(self) -> int:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        return struct.lat
    
    def getType(self) -> int:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        return (struct.typeEncoding >> 4) & 0x0F
    
    def getEncoding(self) -> int:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        return struct.typeEncoding & 0x0F
    
    def getValue(self) -> Any:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        if not struct.ptr:
            return None
        
        encoding = self.getEncoding()
        if encoding == REDIS_OBJECT_ENCODINGS.INT:
            return ctypes.cast(struct.ptr, ctypes.POINTER(ctypes.c_int32))[0]
        else:
            return ctypes.string_at(struct.ptr, struct.size - 1).decode()
    
    def getLRUClock(self) -> int:
        return int(time.time()) & Config.LRU_BITS_MASK
