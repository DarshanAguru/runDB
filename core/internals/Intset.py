import ctypes
import weakref
import random
from .Malloc_internal import MallocInternal

class IntsetStruct(ctypes.Structure):
    _fields_ = [
        ("encoding", ctypes.c_uint32),
        ("length", ctypes.c_uint32),
    ]

class Intset:
    # Encoding constants
    INTSET_ENC_INT16 = 2
    INTSET_ENC_INT32 = 4
    INTSET_ENC_INT64 = 8

    def __init__(self, ptr=None):
        self.has_ownership = True
        if ptr is not None:
            self.ptr = ptr
            self.has_ownership = False
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
            self.size = ctypes.sizeof(IntsetStruct) + struct_obj.length * struct_obj.encoding
        else:
            self.size = ctypes.sizeof(IntsetStruct)
            self.ptr = MallocInternal.zmalloc(self.size)
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
            struct_obj.encoding = self.INTSET_ENC_INT16
            struct_obj.length = 0
            
            self._finalizer = weakref.finalize(
                self,
                MallocInternal.zfree,
                self.ptr
            )

    def release(self) -> int:
        self.has_ownership = False
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
        return self.ptr

    def free(self):
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer()

    def _val_encoding(self, val: int) -> int:
        if -32768 <= val <= 32767:
            return self.INTSET_ENC_INT16
        elif -2147483648 <= val <= 2147483647:
            return self.INTSET_ENC_INT32
        else:
            return self.INTSET_ENC_INT64

    def _get_val_at(self, idx: int, encoding: int) -> int:
        offset = ctypes.sizeof(IntsetStruct) + idx * encoding
        if encoding == self.INTSET_ENC_INT16:
            return ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int16))[0]
        elif encoding == self.INTSET_ENC_INT32:
            return ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int32))[0]
        else:
            return ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int64))[0]

    def _set_val_at(self, idx: int, val: int, encoding: int):
        offset = ctypes.sizeof(IntsetStruct) + idx * encoding
        if encoding == self.INTSET_ENC_INT16:
            ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int16))[0] = val
        elif encoding == self.INTSET_ENC_INT32:
            ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int32))[0] = val
        else:
            ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int64))[0] = val

    def _resize(self, new_size: int):
        if self.has_ownership:
            if hasattr(self, "_finalizer") and self._finalizer.alive:
                self._finalizer.detach()
            new_ptr = MallocInternal.zrealloc(self.ptr, new_size)
            self.ptr = new_ptr
            self._finalizer = weakref.finalize(self, MallocInternal.zfree, self.ptr)
        else:
            new_ptr = MallocInternal.zrealloc(self.ptr, new_size)
            self.ptr = new_ptr
        self.size = new_size

    def find(self, val: int) -> tuple[bool, int]:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        length = struct_obj.length
        if length == 0:
            return False, 0
            
        encoding = struct_obj.encoding
        min_val = self._get_val_at(0, encoding)
        max_val = self._get_val_at(length - 1, encoding)
        if val < min_val:
            return False, 0
        elif val > max_val:
            return False, length
            
        low = 0
        high = length - 1
        while low <= high:
            mid = (low + high) // 2
            mid_val = self._get_val_at(mid, encoding)
            if mid_val == val:
                return True, mid
            elif mid_val < val:
                low = mid + 1
            else:
                high = mid - 1
        return False, low

    def _upgrade_and_insert(self, val: int, new_encoding: int):
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        old_encoding = struct_obj.encoding
        length = struct_obj.length
        
        # New size will fit length + 1 elements of new encoding width
        new_size = ctypes.sizeof(IntsetStruct) + (length + 1) * new_encoding
        self._resize(new_size)
        
        # Re-fetch header pointer in case resize moved it
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        
        # Value goes to index 0 if negative, else index length
        prepend = 1 if val < 0 else 0
        
        # Shift old elements right-to-left
        for i in range(length - 1, -1, -1):
            old_val = self._get_val_at(i, old_encoding)
            self._set_val_at(i + prepend, old_val, new_encoding)
            
        # Write new value
        if prepend:
            self._set_val_at(0, val, new_encoding)
        else:
            self._set_val_at(length, val, new_encoding)
            
        struct_obj.encoding = new_encoding
        struct_obj.length = length + 1

    def add(self, val: int) -> bool:
        if not isinstance(val, int) or isinstance(val, bool):
            raise TypeError("Intset only supports integer values")
            
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        required_encoding = self._val_encoding(val)
        
        if required_encoding > struct_obj.encoding:
            self._upgrade_and_insert(val, required_encoding)
            return True
            
        found, insert_idx = self.find(val)
        if found:
            return False
            
        length = struct_obj.length
        new_size = ctypes.sizeof(IntsetStruct) + (length + 1) * struct_obj.encoding
        self._resize(new_size)
        
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        encoding = struct_obj.encoding
        
        # Shift elements right-to-left
        for i in range(length - 1, insert_idx - 1, -1):
            old_val = self._get_val_at(i, encoding)
            self._set_val_at(i + 1, old_val, encoding)
            
        self._set_val_at(insert_idx, val, encoding)
        struct_obj.length = length + 1
        return True

    def remove(self, val: int) -> bool:
        if not isinstance(val, int) or isinstance(val, bool):
            return False
            
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        if self._val_encoding(val) > struct_obj.encoding:
            return False
            
        found, idx = self.find(val)
        if not found:
            return False
            
        length = struct_obj.length
        encoding = struct_obj.encoding
        
        # Shift elements left-to-right
        for i in range(idx + 1, length):
            next_val = self._get_val_at(i, encoding)
            self._set_val_at(i - 1, next_val, encoding)
            
        new_size = ctypes.sizeof(IntsetStruct) + (length - 1) * encoding
        self._resize(new_size)
        
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        struct_obj.length = length - 1
        return True

    def __len__(self) -> int:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        return struct_obj.length

    def __contains__(self, val: int) -> bool:
        if not isinstance(val, int) or isinstance(val, bool):
            return False
        return self.find(val)[0]

    def __iter__(self):
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        length = struct_obj.length
        encoding = struct_obj.encoding
        for i in range(length):
            yield self._get_val_at(i, encoding)

    def get_random_member(self) -> int:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(IntsetStruct)).contents
        length = struct_obj.length
        if length == 0:
            raise IndexError("Intset is empty")
        idx = random.randint(0, length - 1)
        return self._get_val_at(idx, struct_obj.encoding)
