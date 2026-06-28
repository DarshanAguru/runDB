import ctypes
from .sds import SDS

class BloomFilterStruct(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_char * 4),       # "BLMF"
        ("bits", ctypes.c_int64)
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
            struct_obj.bits = 0
            
    def free(self):
        self.sds_val.free()

    def release(self) -> int:
        self.has_ownership = False
        return self.sds_val.release()
    
    def add(self, data: str | bytes) -> bool:
        if isinstance(data, str):
            b = data.encode('utf-8')
        else:
            b = data
            
        # FNV-1a 64-bit hash
        h = 0xcbf29ce484222325
        for byte in b:
            h ^= byte
            h = (h * 0x100000001b3) & 0xffffffffffffffff
            
        h1 = h & 0xffffffff
        h2 = h >> 32
        
        mask = 0
        for i in range(4): # K_HASHES = 4
            idx = (h1 + i * h2) % 64
            mask |= (1 << idx)
            
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(BloomFilterStruct)).contents
        old_bits = struct_obj.bits
        new_bits = old_bits | mask
        if new_bits != old_bits:
            struct_obj.bits = new_bits
            return True
        return False
            
    def exists(self, data: str | bytes) -> bool:
        if isinstance(data, str):
            b = data.encode('utf-8')
        else:
            b = data
            
        # FNV-1a 64-bit hash
        h = 0xcbf29ce484222325
        for byte in b:
            h ^= byte
            h = (h * 0x100000001b3) & 0xffffffffffffffff
            
        h1 = h & 0xffffffff
        h2 = h >> 32
        
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(BloomFilterStruct)).contents
        bits = struct_obj.bits
        for i in range(4): # K_HASHES = 4
            idx = (h1 + i * h2) % 64
            if not (bits & (1 << idx)):
                return False
        return True
