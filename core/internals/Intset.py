import ctypes
import weakref
import random
from .Malloc_internal import MallocInternal
from typing import Tuple, Any, Callable

class IntsetStruct(ctypes.Structure):
    _fields_ = [
        ("encoding", ctypes.c_uint32),
        ("length", ctypes.c_uint32),
    ]

# Intset implements a memory-efficient integer set with:
# - Contiguous Sorted Array: Elements are stored sorted, enabling O(log N) lookup.
# - Binary Search: Finds elements or insertion slots via standard bisecting logic.
# - Upgrading Encoding: Promotes width (16-bit -> 32-bit -> 64-bit) as values exceed bounds.
# - Memmove Shifting: Relies on ctypes.memmove for shift-left and shift-right operations to bypass Python loops.
class IntSetHelper:
    INT16_ENC = 2
    INT32_ENC = 4
    INT64_ENC = 8

    INT16_RANGE = (-32768, 32767)
    INT32_RANGE = (-2147483648, 2147483647) 
    
    @staticmethod
    def _in_range(val: int, bounds: tuple[int, int]) -> bool:
        return bounds[0] <= val <= bounds[1]
    
    @staticmethod
    def _binary_search(low: int, high: int, midFunc: Callable[[int, int], int], encoding: int, val: int) -> Tuple[bool, int]:
        l = low
        h = high - 1
        while l <= h:
            mid = l + (h - l) // 2
            mid_val = midFunc(mid, encoding)
            if mid_val == val:
                return True, mid
            elif mid_val > val:
                h = mid - 1
            else:
                l = mid + 1
        return False, l



class Intset:
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
            struct_obj.encoding = IntSetHelper.INT16_ENC
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
        if IntSetHelper._in_range(val, IntSetHelper.INT16_RANGE):
            return IntSetHelper.INT16_ENC
        elif IntSetHelper._in_range(val, IntSetHelper.INT32_RANGE):
            return IntSetHelper.INT32_ENC
        else:
            return IntSetHelper.INT64_ENC

    def _get_val_at(self, idx: int, encoding: int) -> int:
        offset = ctypes.sizeof(IntsetStruct) + idx * encoding
        if encoding == IntSetHelper.INT16_ENC:
            return ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int16))[0]
        elif encoding == IntSetHelper.INT32_ENC:
            return ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int32))[0]
        else:
            return ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int64))[0]

    def _set_val_at(self, idx: int, val: int, encoding: int):
        offset = ctypes.sizeof(IntsetStruct) + idx * encoding
        if encoding == IntSetHelper.INT16_ENC:
            ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_int16))[0] = val
        elif encoding == IntSetHelper.INT32_ENC:
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
            
        return IntSetHelper._binary_search(0, length, self._get_val_at, encoding, val)
        

    def _upgrade_and_insert(self, val: int, new_encoding: int):
        # Upgrades the whole intset's element bit-width and moves old elements to new positions
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
        # If value requires larger encoding, trigger upgrade, otherwise insert in-place utilizing memmove to shift
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
        if length > insert_idx:
            src = self.ptr + ctypes.sizeof(IntsetStruct) + insert_idx * encoding
            dst = self.ptr + ctypes.sizeof(IntsetStruct) + (insert_idx + 1) * encoding
            nbytes = (length - insert_idx) * encoding
            ctypes.memmove(dst, src, nbytes)
            
        self._set_val_at(insert_idx, val, encoding)
        struct_obj.length = length + 1
        return True

    def remove(self, val: int) -> bool:
        # Shift subsequent elements left using memmove to fill the deleted slot, then shrink memory buffer
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
        if length - 1 > idx:
            src = self.ptr + ctypes.sizeof(IntsetStruct) + (idx + 1) * encoding
            dst = self.ptr + ctypes.sizeof(IntsetStruct) + idx * encoding
            nbytes = (length - idx - 1) * encoding
            ctypes.memmove(dst, src, nbytes)
            
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
