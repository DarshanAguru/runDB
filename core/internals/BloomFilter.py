from core.internals.Hashers import Hashers
import ctypes
from .sds import SDS
from .Hashers import Hashers

# m = -(n*ln(p) / (ln2 ** 2))
# m = no. of bits, n = expected number of unique items, p = desired false positive rate
# k = (m / n) * ln(2)
# k = number of optimal Hashes
# For n = 100 unique keys, error percent p = 1%
# m = 958.5 bits (we use M = 1000)
# k = 6.64 (we use optimal K = 7)

M = 1000
K = 7
NUM_BYTES = (M + 7) // 8

class BloomFilterStruct(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_char * 4),
        ("bits", ctypes.c_ubyte * NUM_BYTES)
    ]

BF_SIZE = ctypes.sizeof(BloomFilterStruct)


class BloomFilter:
    def __init__(self, ptr=None):
        if ptr is not None:
            self.sds_val = SDS(ptr=ptr)
            self.ptr = ptr
            self.has_ownership = False
        else:
            # Allocate BloomFilter as an SDS string of size BF_SIZE
            self.sds_val = SDS(b"\x00" * BF_SIZE)
            self.ptr = self.sds_val._sds_ptr.ptr
            self.has_ownership = True
            
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(BloomFilterStruct)).contents
            struct_obj.magic = b"BLMF"
            # Memory is already initialized to all 0s via SDS alloc
            
    def free(self):
        self.sds_val.free()

    def release(self) -> int:
        self.has_ownership = False
        return self.sds_val.release()
    
    def add(self, data: str | bytes) -> bool:
        if isinstance(data, str):
            b_data = data.encode('utf-8')
        else:
            b_data = data

        h1 = Hashers.fnv1a(b_data)
        h2 = Hashers.fnv1a(b"rundbsalt" + b_data)
        
        changed = False
        bits_ptr = ctypes.cast(self.ptr + 4, ctypes.POINTER(ctypes.c_ubyte))
        
        for i in range(K):
            bit_idx = (h1 + i * h2) % M
            byte_idx = bit_idx // 8
            bit_pos = bit_idx % 8
            
            mask = 1 << bit_pos
            if (bits_ptr[byte_idx] & mask) == 0:
                bits_ptr[byte_idx] |= mask
                changed = True
            
        return changed
            
    def exists(self, data: str | bytes) -> bool:
        if isinstance(data, str):
            b_data = data.encode('utf-8')
        else:
            b_data = data

        h1 = Hashers.fnv1a(b_data)
        h2 = Hashers.fnv1a(b"rundbsalt" + b_data)
        
        bits_ptr = ctypes.cast(self.ptr + 4, ctypes.POINTER(ctypes.c_ubyte))
        
        for i in range(K):
            bit_idx = (h1 + i * h2) % M
            byte_idx = bit_idx // 8
            bit_pos = bit_idx % 8
            
            mask = 1 << bit_pos
            if (bits_ptr[byte_idx] & mask) == 0:
                return False
        return True
