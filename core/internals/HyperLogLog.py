import ctypes
import math
from .sds import SDS
from .Hashers import Hashers


HLL_REG_BITS = 14
HLL_REG_MASK = (1 << HLL_REG_BITS) - 1
HLL_REGISTERS = 1 << HLL_REG_BITS
CARD_BITS = 64 - HLL_REG_BITS

class HyperLogLogStruct(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_char * 4),       # "HYLL"
        ("encoding", ctypes.c_uint8),       # 0 = dense
        ("card", ctypes.c_uint64),          # cached cardinality
        ("card_valid", ctypes.c_uint8),     # flag: 1 = valid, 0 = invalid (dirty)
        ("registers", ctypes.c_uint8 * HLL_REGISTERS)
    ]

# The total size of the HyperLogLog structure in bytes
HLL_SIZE = ctypes.sizeof(HyperLogLogStruct)


class HyperLogLog:
    def __init__(self, ptr=None):
        if ptr is not None:
            self.sds_val = SDS(ptr=ptr)
            self.ptr = ptr
            self.has_ownership = False
        else:
            # Allocate HLL as an SDS string of size HLL_SIZE
            self.sds_val = SDS(b"\x00" * HLL_SIZE)
            self.ptr = self.sds_val._sds_ptr.ptr
            self.has_ownership = True
            
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HyperLogLogStruct)).contents
            struct_obj.magic = b"HYLL"
            struct_obj.encoding = 0
            struct_obj.card = 0
            struct_obj.card_valid = 1

    def free(self):
        self.sds_val.free()

    def release(self) -> int:
        self.has_ownership = False
        return self.sds_val.release()

    def add(self, element: bytes) -> bool:
        h = Hashers.murmur64a(element)
        idx = h & HLL_REG_MASK       # Lower 14 bits for register index (16384 registers)
        val = h >> HLL_REG_BITS      # Remaining 50 bits
        
        if val == 0:
            zeros = CARD_BITS
        else:
            zeros = CARD_BITS - val.bit_length()
        rank = zeros + 1
        
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HyperLogLogStruct)).contents
        if rank > struct_obj.registers[idx]:
            struct_obj.registers[idx] = rank
            struct_obj.card_valid = 0
            return True
        return False

    def count(self) -> int:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HyperLogLogStruct)).contents
        if struct_obj.card_valid == 1:
            return struct_obj.card
            
        m = HLL_REGISTERS
        alpha = 0.7213 / (1.0 + 1.079 / m)
        
        sum_val = 0.0
        for r in struct_obj.registers:
            sum_val += math.ldexp(1.0, -r)  # 2^-r
            
        # Raw estimate
        E = alpha * (m ** 2) / sum_val
        
        # Low cardinality correction (Linear Counting)
        if E <= 2.5 * m:
            V = 0
            for r in struct_obj.registers:
                if r == 0:
                    V += 1
            if V > 0:
                E = m * math.log(m / V)
                
        # Cache the result
        struct_obj.card = round(E)
        struct_obj.card_valid = 1
        return struct_obj.card

    def merge(self, other_hlls: list['HyperLogLog']) -> None:
        struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(HyperLogLogStruct)).contents
        modified = False
        
        # Get ctypes register arrays of all other HLLs
        other_regs = [ctypes.cast(o.ptr, ctypes.POINTER(HyperLogLogStruct)).contents.registers for o in other_hlls]
        
        for i in range(HLL_REGISTERS):
            max_r = struct_obj.registers[i]
            for regs in other_regs:
                if regs[i] > max_r:
                    max_r = regs[i]
                    modified = True
            struct_obj.registers[i] = max_r
            
        if modified:
            struct_obj.card_valid = 0