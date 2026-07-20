import ctypes
import weakref
import struct
from .Malloc_internal import MallocInternal
from .Hashers import Hashers
from typing import Any


class HashMapStruct(ctypes.Structure):
    _fields_ = [
        ("capacity", ctypes.c_uint32),
        ("count", ctypes.c_uint32),
    ]

# HashMap Helper provides FNV-1a hashing (via Hashers) and state flag definitions:
# - FNV-1a: 32-bit hash providing fast, well-distributed keyspace dispersion.
# - Open Addressing & Linear Probing: Resolves slot collisions by incrementing slots sequentially.
# - Tombstoning: State flags (EMPTY, OCCUPIED, TOMBSTONE) allow deleting keys without breaking 
#   the search/probing sequence of subsequently inserted keys.
class HashMapHelper:
    DEFAULT_CAPACITY = 16
    CAPACITY_INC_THRESHOLD = 0.7

    EMPTY_STATE     = 0
    OCCUPIED_STATE  = 1
    TOMBSTONE_STATE = 2

    @staticmethod
    def hashKey(key_bytes: bytes) -> int:
        return Hashers.fnv1a(key_bytes)

class HashMap:
    def __init__(self, keyType: str, valType: str, ptr=None):
        # Maps existing memory if pointer is supplied, otherwise allocates new zero-filled block
        self.keyType = self._get_type(keyType)
        self.valType = self._get_type(valType)
        self._bucket_class = self._make_bucket_class(self.keyType, self.valType)
        
        if ptr is not None:
            self.ptr = ptr
            self.has_ownership = False
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
            self.size = ctypes.sizeof(HashMapStruct) + struct_obj.capacity * ctypes.sizeof(self._bucket_class)
        else:
            capacity = HashMapHelper.DEFAULT_CAPACITY
            self.size = self._get_size(keyType, valType, capacity)
            self.ptr = MallocInternal.zcalloc(self.size)
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
            struct_obj.capacity = capacity
            struct_obj.count = 0
            self.has_ownership = True
            
            self._finalizer = weakref.finalize(
                self,
                self._cleanup,
                self.ptr,
                self.size,
                self.keyType == ctypes.c_void_p,
                self.valType == ctypes.c_void_p,
                self._bucket_class
            )

    @staticmethod
    def _cleanup(ptr, size, free_keys, free_vals, bucket_class):
        # Deallocates dynamically allocated string keys and values before freeing the table itself
        if not ptr:
            return
        
        header = ctypes.cast(ptr, ctypes.POINTER(HashMapStruct)).contents
        capacity = header.capacity
        
        # Free allocated string keys/values
        if free_keys or free_vals:
            from .sds import SDS
            header_size = ctypes.sizeof(HashMapStruct)
            bucket_size = ctypes.sizeof(bucket_class)
            for i in range(capacity):
                b_ptr = ptr + header_size + i * bucket_size
                bucket = ctypes.cast(b_ptr, ctypes.POINTER(bucket_class)).contents
                if bucket.state == HashMapHelper.OCCUPIED_STATE:  # Occupied
                    if free_keys and bucket.key:
                        SDS(ptr=bucket.key).free()
                    if free_vals and bucket.val:
                        SDS(ptr=bucket.val).free()
                        
        MallocInternal.zfree(ptr)

    def free(self):
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer()

    def release(self) -> int:
        # Detaches finalizer to transfer ownership of the allocated C structure to the caller
        self.has_ownership = False
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
        return self.ptr

    def _get_type(self, type_str: str):
        type_str = type_str.lower()
        if type_str in ("int", "int32"):
            return ctypes.c_int32
        elif type_str == "int64":
            return ctypes.c_int64
        elif type_str == "double":
            return ctypes.c_double
        elif type_str in ("string", "str", "bytes"):
            return ctypes.c_void_p
        else:
            raise ValueError(f"Unsupported type: {type_str}")

    def _make_bucket_class(self, k_ctype, v_ctype):
        class Bucket(ctypes.Structure):
            _fields_ = [
                ("state", ctypes.c_uint8),
                ("key", k_ctype),
                ("val", v_ctype)
            ]
        return Bucket

    def _get_size(self, keyType: str, valType: str, capacity: int) -> int:
        k_ctype = self._get_type(keyType)
        v_ctype = self._get_type(valType)
        bucket_size = ctypes.sizeof(self._make_bucket_class(k_ctype, v_ctype))
        header_size = ctypes.sizeof(HashMapStruct)
        return header_size + capacity * bucket_size

    def _is_type(self, ctype, val: Any) -> bool:
        if ctype in (ctypes.c_int32, ctypes.c_int64):
            return isinstance(val, int) and not isinstance(val, bool)
        elif ctype == ctypes.c_double:
            return isinstance(val, (int, float)) and not isinstance(val, bool)
        elif ctype == ctypes.c_void_p:
            return isinstance(val, (str, bytes))
        return False

    def _hash(self, key) -> int:
        if isinstance(key, str):
            key_bytes = key.encode()
        elif isinstance(key, bytes):
            key_bytes = key
        elif isinstance(key, int):
            key_bytes = struct.pack("<q", key)
        elif isinstance(key, float):
            key_bytes = struct.pack("<d", key)
        else:
            key_bytes = str(key).encode()
        
        return HashMapHelper.hashKey(key_bytes)

    def _get_bucket(self, index: int):
        header_size = ctypes.sizeof(HashMapStruct)
        bucket_size = ctypes.sizeof(self._bucket_class)
        b_ptr = self.ptr + header_size + index * bucket_size
        return ctypes.cast(b_ptr, ctypes.POINTER(self._bucket_class)).contents

    def _decode_key(self, key_field):
        if self.keyType == ctypes.c_void_p:
            if not key_field:
                return None
            from .sds import SDS
            b = bytes(SDS(ptr=key_field))
            try:
                return b.decode()
            except UnicodeDecodeError:
                return b
        else:
            return key_field

    def _decode_val(self, val_field):
        if self.valType == ctypes.c_void_p:
            if not val_field:
                return None
            from .sds import SDS
            b = bytes(SDS(ptr=val_field))
            try:
                return b.decode()
            except UnicodeDecodeError:
                return b
        else:
            return val_field

    def _encode_key(self, key) -> tuple[Any, int]:
        # For string types, copies Python string bytes into native C-heap memory block (+1 for null-terminator)
        if self.keyType == ctypes.c_void_p:
            b = key.encode() if isinstance(key, str) else key
            from .sds import SDS
            sds_obj = SDS(b)
            return sds_obj.release(), len(b)
        else:
            return key, 0

    def _encode_val(self, val) -> tuple[Any, int]:
        if self.valType == ctypes.c_void_p:
            b = val.encode() if isinstance(val, str) else val
            from .sds import SDS
            sds_obj = SDS(b)
            return sds_obj.release(), len(b)
        else:
            return val, 0

    def set(self, key: Any, val: Any):
        if not self._is_type(self.keyType, key):
            raise KeyError(f"Key {key} is not of expected type")
        if not self._is_type(self.valType, val):
            raise ValueError(f"Value {val} is not of expected type")
            
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        
        # Check load factor and resize if needed
        if ( (struct_obj.count + 1) / struct_obj.capacity ) > HashMapHelper.CAPACITY_INC_THRESHOLD:
            self._resize(struct_obj.capacity * 2)
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
            
        h = self._hash(key)
        capacity = struct_obj.capacity
        slot = h % capacity
        
        # Implements open-addressing insert with linear probing, inserting at first encountered tombstone or empty slot
        first_tombstone_slot = None
        
        while True:
            bucket = self._get_bucket(slot)
            
            if bucket.state == HashMapHelper.EMPTY_STATE:
                target_slot = first_tombstone_slot if first_tombstone_slot is not None else slot
                target_bucket = self._get_bucket(target_slot)
                
                enc_key, _ = self._encode_key(key)
                enc_val, _ = self._encode_val(val)
                target_bucket.state = 1
                target_bucket.key = enc_key
                target_bucket.val = enc_val
                
                struct_obj.count += 1
                return
                
            elif bucket.state == HashMapHelper.OCCUPIED_STATE:
                b_key = self._decode_key(bucket.key)
                if b_key == key:
                    if self.valType == ctypes.c_void_p and bucket.val:
                        from .sds import SDS
                        SDS(ptr=bucket.val).free()
                    enc_val, _ = self._encode_val(val)
                    bucket.val = enc_val
                    return
                    
            elif bucket.state == HashMapHelper.TOMBSTONE_STATE:
                if first_tombstone_slot is None:
                    first_tombstone_slot = slot
                    
            slot = (slot + 1) % capacity

    def get(self, key: Any, default: Any = None) -> Any:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        if struct_obj.count == 0:
            return default
            
        h = self._hash(key)
        capacity = struct_obj.capacity
        slot = h % capacity
        
        start = slot
        while True:
            bucket = self._get_bucket(slot)
            
            if bucket.state == HashMapHelper.EMPTY_STATE:
                return default
            elif bucket.state == HashMapHelper.OCCUPIED_STATE:
                b_key = self._decode_key(bucket.key)
                if b_key == key:
                    return self._decode_val(bucket.val)
            
            slot = (slot + 1) % capacity
            if slot == start:
                break
        return default

    def delete(self, key: Any) -> bool:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        if struct_obj.count == 0:
            return False
            
        h = self._hash(key)
        capacity = struct_obj.capacity
        slot = h % capacity
        
        start = slot
        while True:
            bucket = self._get_bucket(slot)
            
            if bucket.state == HashMapHelper.EMPTY_STATE:
                return False
            elif bucket.state == HashMapHelper.OCCUPIED_STATE:
                b_key = self._decode_key(bucket.key)
                if b_key == key:
                    from .sds import SDS
                    if self.keyType == ctypes.c_void_p and bucket.key:
                        SDS(ptr=bucket.key).free()
                        bucket.key = None
                    else:
                        bucket.key = 0
                        
                    if self.valType == ctypes.c_void_p and bucket.val:
                        SDS(ptr=bucket.val).free()
                        bucket.val = None
                    else:
                        bucket.val = 0
                    
                    bucket.state = HashMapHelper.TOMBSTONE_STATE
                    struct_obj.count -= 1
                    return True
            
            slot = (slot + 1) % capacity
            if slot == start:
                break
        return False

    def clear(self):
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        capacity = struct_obj.capacity
        from .sds import SDS
        for i in range(capacity):
            bucket = self._get_bucket(i)
            if bucket.state == HashMapHelper.OCCUPIED_STATE:
                if self.keyType == ctypes.c_void_p and bucket.key:
                    SDS(ptr=bucket.key).free()
                if self.valType == ctypes.c_void_p and bucket.val:
                    SDS(ptr=bucket.val).free()
            bucket.state = HashMapHelper.EMPTY_STATE
            if self.keyType == ctypes.c_void_p:
                bucket.key = None
            else:
                bucket.key = 0
            if self.valType == ctypes.c_void_p:
                bucket.val = None
            else:
                bucket.val = 0
        struct_obj.count = 0

    def _resize(self, new_capacity: int):
        # Doubles table size, rehashes all occupied entries, and configures new weakref cleanup finalizer
        bucket_size = ctypes.sizeof(self._bucket_class)
        new_size = ctypes.sizeof(HashMapStruct) + new_capacity * bucket_size
        new_ptr = MallocInternal.zcalloc(new_size)
        
        new_header = ctypes.cast(new_ptr, ctypes.POINTER(HashMapStruct)).contents
        new_header.capacity = new_capacity
        new_header.count = 0
        
        old_header = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        old_capacity = old_header.capacity
        
        header_size = ctypes.sizeof(HashMapStruct)
        for i in range(old_capacity):
            old_bucket = self._get_bucket(i)
            if old_bucket.state == HashMapHelper.OCCUPIED_STATE:
                key_val = self._decode_key(old_bucket.key)
                h = self._hash(key_val)
                slot = h % new_capacity
                while True:
                    new_b_ptr = new_ptr + header_size + slot * bucket_size
                    new_bucket = ctypes.cast(new_b_ptr, ctypes.POINTER(self._bucket_class)).contents
                    if new_bucket.state == HashMapHelper.EMPTY_STATE:
                        new_bucket.state = HashMapHelper.OCCUPIED_STATE
                        new_bucket.key = old_bucket.key
                        new_bucket.val = old_bucket.val
                        new_header.count += 1
                        break
                    slot = (slot + 1) % new_capacity
        
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
        
        MallocInternal.zfree(self.ptr)
        
        self.ptr = new_ptr
        self.size = new_size
        
        self._finalizer = weakref.finalize(
            self,
            self._cleanup,
            self.ptr,
            self.size,
            self.keyType == ctypes.c_void_p,
            self.valType == ctypes.c_void_p,
            self._bucket_class
        )

    def __len__(self) -> int:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        return struct_obj.count

    def __contains__(self, key: Any) -> bool:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        if struct_obj.count == 0:
            return False
            
        h = self._hash(key)
        capacity = struct_obj.capacity
        slot = h % capacity
        
        start = slot
        while True:
            bucket = self._get_bucket(slot)
            
            if bucket.state == HashMapHelper.EMPTY_STATE:
                return False
            elif bucket.state == HashMapHelper.OCCUPIED_STATE:
                b_key = self._decode_key(bucket.key)
                if b_key == key:
                    return True
            
            slot = (slot + 1) % capacity
            if slot == start:
                break
        return False

    def __iter__(self):
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        capacity = struct_obj.capacity
        for i in range(capacity):
            bucket = self._get_bucket(i)
            if bucket.state == HashMapHelper.OCCUPIED_STATE:
                yield self._decode_key(bucket.key)

    def keys(self) -> list:
        return list(self)

    def values(self) -> list:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        capacity = struct_obj.capacity
        res = []
        for i in range(capacity):
            bucket = self._get_bucket(i)
            if bucket.state == HashMapHelper.OCCUPIED_STATE:
                res.append(self._decode_val(bucket.val))
        return res

    def items(self) -> list[tuple]:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        capacity = struct_obj.capacity
        res = []
        for i in range(capacity):
            bucket = self._get_bucket(i)
            if bucket.state == HashMapHelper.OCCUPIED_STATE:
                res.append((self._decode_key(bucket.key), self._decode_val(bucket.val)))
        return res

    def __getitem__(self, key: Any) -> Any:
        val = self.get(key)
        if val is None and key not in self:
            raise KeyError(key)
        return val

    def __setitem__(self, key: Any, val: Any):
        self.set(key, val)

    def __delitem__(self, key: Any):
        if not self.delete(key):
            raise KeyError(key)

    def get_random_key(self) -> Any:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        if struct_obj.count == 0:
            raise KeyError("HashMap is empty")
        capacity = struct_obj.capacity
        for i in range(capacity):
            bucket = self._get_bucket(i)
            if bucket.state == HashMapHelper.OCCUPIED_STATE:
                return self._decode_key(bucket.key)
        raise KeyError("HashMap is empty")

    def get_random_item(self) -> tuple[Any, Any]:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HashMapStruct)).contents
        if struct_obj.count == 0:
            raise KeyError("HashMap is empty")
        capacity = struct_obj.capacity
        for i in range(capacity):
            bucket = self._get_bucket(i)
            if bucket.state == HashMapHelper.OCCUPIED_STATE:
                return self._decode_key(bucket.key), self._decode_val(bucket.val)
        raise KeyError("HashMap is empty")
