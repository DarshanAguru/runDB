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
    TYPE_LIST: int = 1
    TYPE_SET: int = 2
    TYPE_GEO: int = 3
    
# Redis object encoding definitions
class REDIS_OBJECT_ENCODINGS:
    RAW: int = 0
    INT: int = 1
    EMBSTR: int = 8
    QUICKLIST: int = 9
    INTSET: int = 11
    HT: int = 12


class RedisObjectStruct(ctypes.Structure):
    _fields_ = [
        ("typeEncoding", ctypes.c_uint8),
        ("lat", ctypes.c_uint32),
        ("ptr", ctypes.c_void_p),
        ("size", ctypes.c_size_t)
    ]

class StructPtr:
    __slots__ = ["ptr"]
    def __init__(self, ptr: int):
        self.ptr = ptr

# RedisObject represents a unified header and value structure mapped on the C-heap:
# - Type & Encoding bit-packing: Stores object type and encoding in a single c_uint8 field.
# - LRU Clock (LAT): Tracks last access timestamp (LAT) for eviction strategies (LRU/LFU).
# - Memory-Efficient Slots: Utilizes __slots__ to eliminate per-instance Python dict overhead.
# - Polymorphic Value Cleanup: Dynamically cleans up nested structures (QuickLists, Set HashTables)
#   via weakref finalize callbacks.
class RedisObject:
    # Using slots to minimize memory overhead per object, supporting weak references for finalizers
    __slots__ = ["_struct_ptr", "_finalizer", "__weakref__"]

    def __init__(self, val: Any = None, o_type: int = 0, o_encoding: int = 0, ptr=None) -> None:
        if ptr is not None:
            self._struct_ptr = StructPtr(ptr)
            self._finalizer = None
        else:
            # Allocating RedisObjectStruct itself in native C memory
            self._struct_ptr = Malloc.alloc_struct(RedisObjectStruct)
            
            # Populating standard fields
            struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
            struct.typeEncoding = ((o_type & 0x0F) << 4) | (o_encoding & 0x0F)
            struct.lat = self.getLRUClock()
            struct.ptr = None
            struct.size = 0

            # Registering finalizer to ensure both the nested value pointer and the struct itself are freed
            self._finalizer = weakref.finalize(
                self,
                self._cleanup,
                self._struct_ptr
            )

            # Calling property setter to allocate and set the inner value pointer
            self.val = val

    @staticmethod
    def _cleanup(struct_ptr) -> None:
        if struct_ptr and struct_ptr.ptr:
            struct = ctypes.cast(struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
            if struct.ptr:
                o_type = (struct.typeEncoding >> 4) & 0x0F
                if o_type == REDIS_OBJECT_TYPES.TYPE_LIST:
                    from .internals.QuickList import QuickListStruct, QuickListNodeStruct
                    ql_struct = ctypes.cast(struct.ptr, ctypes.POINTER(QuickListStruct)).contents
                    curr_ptr = ql_struct.head
                    while curr_ptr:
                        curr_node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                        next_ptr = curr_node.next
                        if curr_node.zl:
                            MallocInternal.zfree(curr_node.zl)
                        MallocInternal.zfree(curr_ptr)
                        curr_ptr = next_ptr
                    MallocInternal.zfree(struct.ptr)
                elif o_type == REDIS_OBJECT_TYPES.TYPE_SET:
                    o_enc = struct.typeEncoding & 0x0F
                    if o_enc == REDIS_OBJECT_ENCODINGS.INTSET:
                        MallocInternal.zfree(struct.ptr)
                    elif o_enc == REDIS_OBJECT_ENCODINGS.HT:
                        from .internals.HashTable import HashTable
                        ht = HashTable("string", ptr=struct.ptr)
                        ht.map.has_ownership = True
                        ht.map.free()
                elif o_type == REDIS_OBJECT_TYPES.TYPE_GEO:
                    from .internals.HashMap import HashMap
                    hm = HashMap("string", "int64", ptr=struct.ptr)
                    for key, val_ptr in hm.items():
                        if val_ptr:
                            MallocInternal.zfree(val_ptr)
                    hm.has_ownership = True
                    hm.free()
                else:
                    MallocInternal.zfree(struct.ptr)
            
            # Detach finalizer if it exists (i.e. is MallocInternal)
            if hasattr(struct_ptr, "_finalizer") and struct_ptr._finalizer:
                struct_ptr._finalizer.detach()
            MallocInternal.zfree(struct_ptr.ptr)

    def free(self) -> None:
        if hasattr(self, "_finalizer") and self._finalizer and self._finalizer.alive:
            self._finalizer()
        else:
            self._cleanup(self._struct_ptr)

    def release(self) -> int:
        if hasattr(self, "_finalizer") and self._finalizer and self._finalizer.alive:
            self._finalizer.detach()
        if hasattr(self._struct_ptr, "_finalizer") and self._struct_ptr._finalizer and self._struct_ptr._finalizer.alive:
            self._struct_ptr._finalizer.detach()
        return self._struct_ptr.ptr

    # Getter, gets the ptr for struct and dereferences it and returns the content
    @property
    def val(self) -> Any:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        return struct

    # Setter, updates/allocates memory for the value based on the type
    @val.setter
    def val(self, new_val: Any) -> None:
        struct = ctypes.cast(self._struct_ptr.ptr, ctypes.POINTER(RedisObjectStruct)).contents
        
        # Free existing data pointer if present
        if struct.ptr:
            o_type = (struct.typeEncoding >> 4) & 0x0F
            if o_type == REDIS_OBJECT_TYPES.TYPE_LIST:
                from .internals.QuickList import QuickListStruct, QuickListNodeStruct
                ql_struct = ctypes.cast(struct.ptr, ctypes.POINTER(QuickListStruct)).contents
                curr_ptr = ql_struct.head
                while curr_ptr:
                    curr_node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                    next_ptr = curr_node.next
                    if curr_node.zl:
                        MallocInternal.zfree(curr_node.zl)
                    MallocInternal.zfree(curr_ptr)
                    curr_ptr = next_ptr
                MallocInternal.zfree(struct.ptr)
            elif o_type == REDIS_OBJECT_TYPES.TYPE_SET:
                o_enc = struct.typeEncoding & 0x0F
                if o_enc == REDIS_OBJECT_ENCODINGS.INTSET:
                    MallocInternal.zfree(struct.ptr)
                elif o_enc == REDIS_OBJECT_ENCODINGS.HT:
                    from .internals.HashTable import HashTable
                    ht = HashTable("string", ptr=struct.ptr)
                    ht.map.has_ownership = True
                    ht.map.free()
            elif o_type == REDIS_OBJECT_TYPES.TYPE_GEO:
                from .internals.HashMap import HashMap
                hm = HashMap("string", "int64", ptr=struct.ptr)
                for key, val_ptr in hm.items():
                    if val_ptr:
                        MallocInternal.zfree(val_ptr)
                hm.has_ownership = True
                hm.free()
            else:
                MallocInternal.zfree(struct.ptr)
            struct.ptr = None
            struct.size = 0

        if new_val is None:
            return

        o_type = (struct.typeEncoding >> 4) & 0x0F
        if o_type == REDIS_OBJECT_TYPES.TYPE_GEO:
            struct.ptr = new_val.release()
            struct.size = new_val.size
            return

        encoding = self.getEncoding()
        if encoding == REDIS_OBJECT_ENCODINGS.INT:
            size = ctypes.sizeof(ctypes.c_int32)
            ptr = MallocInternal.zmalloc(size)
            ctypes.cast(ptr, ctypes.POINTER(ctypes.c_int32))[0] = int(new_val)
            struct.ptr = ptr
            struct.size = size
        elif encoding == REDIS_OBJECT_ENCODINGS.QUICKLIST:
            from .internals.QuickList import QuickListStruct
            struct.ptr = new_val.release()
            struct.size = ctypes.sizeof(QuickListStruct)
        elif encoding in (REDIS_OBJECT_ENCODINGS.INTSET, REDIS_OBJECT_ENCODINGS.HT):
            struct.ptr = new_val.release()
            struct.size = new_val.underlying.size if encoding == REDIS_OBJECT_ENCODINGS.INTSET else new_val.underlying.map.size
        else:
            if isinstance(new_val, str):
                data = new_val.encode()
            else:
                data = bytes(new_val)
            size = len(data) + 1
            ptr = MallocInternal.zmalloc(size)
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
        
        o_type = self.getType()
        if o_type == REDIS_OBJECT_TYPES.TYPE_GEO:
            from .internals.HashMap import HashMap
            return HashMap("string", "int64", ptr=struct.ptr)
        
        encoding = self.getEncoding()
        if encoding == REDIS_OBJECT_ENCODINGS.INT:
            return ctypes.cast(struct.ptr, ctypes.POINTER(ctypes.c_int32))[0]
        elif encoding == REDIS_OBJECT_ENCODINGS.QUICKLIST:
            from .internals.QuickList import QuickList
            return QuickList(ptr=struct.ptr)
        elif encoding in (REDIS_OBJECT_ENCODINGS.INTSET, REDIS_OBJECT_ENCODINGS.HT):
            from .internals.Set import Set
            return Set(ptr=struct.ptr, encoding=encoding)
        else:
            return ctypes.string_at(struct.ptr, struct.size - 1).decode()
    
    def getLRUClock(self) -> int:
        return int(time.time()) & Config.LRU_BITS_MASK
