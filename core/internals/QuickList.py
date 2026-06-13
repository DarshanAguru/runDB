import ctypes
import weakref
import struct
from .Malloc import Malloc
from .Malloc_internal import MallocInternal


class ZipListHelper:
    ZL_SIZE_HEADER_SIZE = 4
    ZL_TAIL_OFFSET_HEADER_SIZE = 4
    ZL_LEN_HEADER_SIZE = 4
    ZL_HEADER_SIZE = 12
    
    # 0xFF (bin: 11111111) - End marker indicating the end of the ZipList.
    END_MARKER = 0xFF
    
    # 254 (bin: 11111110) - Marker in prev_len field to indicate previous length is >= 254 bytes.
    PREV_LEN_LARGER_SIZE_MARKER = 254

    # Ranges for integer compression
    INT4_RANGE  = (0, 12)
    INT8_RANGE  = (-128, 127)
    INT16_RANGE = (-32768, 32767)
    INT24_RANGE = (-8388608, 8388607)
    INT32_RANGE = (-2147483648, 2147483647)
    INT64_RANGE = (-9223372036854775808, 9223372036854775807)

    # Ranges for string length encoding
    STRING6_RANGE  = (0, 63)
    STRING14_RANGE = (64, 16383)

    # Encoding prefixes:
    # 0xF0 (bin: 11110000) - Prefix for 24-bit signed integer or 4-bit immediate integer encoding base.
    INT4_ENC = 0xF0
    INT24_ENC = 0xF0
    
    # 0xFE (bin: 11111110) - Prefix for 8-bit signed integer encoding.
    INT8_ENC = 0xFE
    
    # 0xC0 (bin: 11000000) - Prefix for 16-bit signed integer encoding.
    INT16_ENC = 0xC0
    
    # 0xD0 (bin: 11010000) - Prefix for 32-bit signed integer encoding.
    INT32_ENC = 0xD0
    
    # 0xE0 (bin: 11100000) - Prefix for 64-bit signed integer encoding.
    INT64_ENC = 0xE0

    # 0x40 (bin: 01000000) - Prefix mask for 14-bit length string encoding.
    STRING14_ENC = 0x40
    
    # 0x80 (bin: 10000000) - Prefix for 32-bit length string encoding.
    STRING32_ENC = 0x80

    # Masks for decoding:
    # 0xC0 (bin: 11000000) - Mask for checking the top 2 bits of the encoding byte.
    MASK_TOP_TWO_BITS = 0xC0
    
    # 0xF0 (bin: 11110000) - Mask for checking the top 4 bits of the encoding byte.
    MASK_TOP_FOUR_BITS = 0xF0

    @staticmethod
    def _in_range(val: int, bounds: tuple[int, int]) -> bool:
        return bounds[0] <= val <= bounds[1]

    @staticmethod
    def encode_prev_len(prev_len: int) -> bytes:
        if prev_len < ZipListHelper.PREV_LEN_LARGER_SIZE_MARKER:
            return bytes([prev_len])
        else:
            return bytes([ZipListHelper.PREV_LEN_LARGER_SIZE_MARKER]) + struct.pack("<I", prev_len)

    @staticmethod
    def decode_prev_len(ptr: int, offset: int) -> tuple[int, int]:
        first_byte = ctypes.cast(ptr + offset, ctypes.POINTER(ctypes.c_uint8))[0]
        if first_byte < ZipListHelper.PREV_LEN_LARGER_SIZE_MARKER:
            return first_byte, 1
        else:
            val = struct.unpack("<I", ctypes.string_at(ptr + offset + 1, 4))[0]
            return val, 5

    @staticmethod
    def try_parse_int(data: bytes) -> int | None:
        try:
            s = data.decode()
            if not s:
                return None
            if s == "0":
                return 0
            if s[0] == '-':
                if len(s) > 1 and s[1] != '0' and s[1:].isdigit():
                    return int(s)
            elif s.isdigit() and s[0] != '0':
                return int(s)
            return None
        except Exception:
            return None

    @staticmethod
    def encode_entry_payload(data: bytes) -> tuple[bytes, bytes]:
        val = ZipListHelper.try_parse_int(data)
        if val is not None:
            if ZipListHelper._in_range(val, ZipListHelper.INT4_RANGE):
                # 4-bit immediate integer encoding: 0xF1 to 0xFD
                return bytes([ZipListHelper.INT4_ENC | (val + 1)]), b""
            elif ZipListHelper._in_range(val, ZipListHelper.INT8_RANGE):
                # 8-bit signed integer: 0xFE followed by 1 byte
                return bytes([ZipListHelper.INT8_ENC]), struct.pack("<b", val)
            elif ZipListHelper._in_range(val, ZipListHelper.INT16_RANGE):
                # 16-bit signed integer: 0xC0 followed by 2 bytes
                return bytes([ZipListHelper.INT16_ENC]), struct.pack("<h", val)
            elif ZipListHelper._in_range(val, ZipListHelper.INT24_RANGE):
                # 24-bit signed integer: 0xF0 followed by 3 bytes
                packed = struct.pack("<i", val)
                return bytes([ZipListHelper.INT24_ENC]), packed[:3]
            elif ZipListHelper._in_range(val, ZipListHelper.INT32_RANGE):
                # 32-bit signed integer: 0xD0 followed by 4 bytes
                return bytes([ZipListHelper.INT32_ENC]), struct.pack("<i", val)
            elif ZipListHelper._in_range(val, ZipListHelper.INT64_RANGE):
                # 64-bit signed integer: 0xE0 followed by 8 bytes
                return bytes([ZipListHelper.INT64_ENC]), struct.pack("<q", val)
                
        # String encoding
        L = len(data)
        if ZipListHelper._in_range(L, ZipListHelper.STRING6_RANGE):
            # 6-bit string length: 00xxxxxx followed by data
            return bytes([L]), data
        elif ZipListHelper._in_range(L, ZipListHelper.STRING14_RANGE):
            # 14-bit string length: 01xxxxxx yyyyyyyy followed by data
            return bytes([ZipListHelper.STRING14_ENC | (L >> 8), L & ZipListHelper.END_MARKER]), data
        else:
            # 32-bit string length: 0x80 followed by 4-byte big-endian length + data
            return bytes([ZipListHelper.STRING32_ENC]) + struct.pack(">I", L), data

    @staticmethod
    def decode_entry_payload(ptr: int, offset: int) -> tuple[bytes, int]:
        first_byte = ctypes.cast(ptr + offset, ctypes.POINTER(ctypes.c_uint8))[0]
        
        # Check if integer/special encoding (first two bits are 11)
        if (first_byte & ZipListHelper.MASK_TOP_TWO_BITS) == ZipListHelper.MASK_TOP_TWO_BITS:
            if (first_byte & ZipListHelper.MASK_TOP_FOUR_BITS) == ZipListHelper.MASK_TOP_FOUR_BITS:
                if first_byte == ZipListHelper.INT8_ENC:
                    val = struct.unpack("<b", ctypes.string_at(ptr + offset + 1, 1))[0]
                    return str(val).encode(), 2
                elif first_byte == ZipListHelper.INT24_ENC:
                    b3 = ctypes.string_at(ptr + offset + 1, 3)
                    pad = b"\xff" if (b3[2] & 0x80) else b"\x00"
                    val = struct.unpack("<i", b3 + pad)[0]
                    return str(val).encode(), 4
                elif 0xF1 <= first_byte <= 0xFD:
                    val = (first_byte & 0x0F) - 1
                    return str(val).encode(), 1
            elif first_byte == ZipListHelper.INT16_ENC:
                val = struct.unpack("<h", ctypes.string_at(ptr + offset + 1, 2))[0]
                return str(val).encode(), 3
            elif first_byte == ZipListHelper.INT32_ENC:
                val = struct.unpack("<i", ctypes.string_at(ptr + offset + 1, 4))[0]
                return str(val).encode(), 5
            elif first_byte == ZipListHelper.INT64_ENC:
                val = struct.unpack("<q", ctypes.string_at(ptr + offset + 1, 8))[0]
                return str(val).encode(), 9
                
        # String encoding (first two bits are 00, 01, or 10)
        if (first_byte & ZipListHelper.MASK_TOP_TWO_BITS) == 0x00:
            length = first_byte & 0x3F
            data = ctypes.string_at(ptr + offset + 1, length)
            return data, 1 + length
        elif (first_byte & ZipListHelper.MASK_TOP_TWO_BITS) == ZipListHelper.STRING14_ENC:
            second_byte = ctypes.cast(ptr + offset + 1, ctypes.POINTER(ctypes.c_uint8))[0]
            length = ((first_byte & 0x3F) << 8) | second_byte
            data = ctypes.string_at(ptr + offset + 2, length)
            return data, 2 + length
        elif first_byte == ZipListHelper.STRING32_ENC:
            length = struct.unpack(">I", ctypes.string_at(ptr + offset + 1, 4))[0]
            data = ctypes.string_at(ptr + offset + 5, length)
            return data, 5 + length
            
        raise ValueError(f"Unknown ziplist entry encoding byte: {hex(first_byte)}")


class ZipListEntry:
    def __init__(self, data: bytes, prev_len: int = 0):
        if isinstance(data, str):
            data = data.encode()
        self.data = data
        self.prev_len = prev_len


class ZipList:
    def __init__(self, ptr=None):
        if ptr is not None:
            self.ptr = ptr
            self.has_ownership = False
        else:
            # Empty ziplist: 12 bytes header + 1 byte end marker = 13 bytes
            self.ptr = MallocInternal.zmalloc(ZipListHelper.ZL_HEADER_SIZE + 1)
            self._write_header(ZipListHelper.ZL_HEADER_SIZE + 1, ZipListHelper.ZL_HEADER_SIZE, 0)
            ctypes.cast(self.ptr + ZipListHelper.ZL_HEADER_SIZE, ctypes.POINTER(ctypes.c_uint8))[0] = ZipListHelper.END_MARKER
            self.has_ownership = True
            self._finalizer = weakref.finalize(self, MallocInternal.zfree, self.ptr)

    @property
    def size(self) -> int:
        return ctypes.cast(self.ptr, ctypes.POINTER(ctypes.c_uint32))[0]

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

    def release(self) -> int:
        self.has_ownership = False
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
        return self.ptr

    def _read_header(self):
        zlbytes = ctypes.cast(self.ptr, ctypes.POINTER(ctypes.c_uint32))[0]
        zltail = ctypes.cast(self.ptr + ZipListHelper.ZL_SIZE_HEADER_SIZE, ctypes.POINTER(ctypes.c_uint32))[0]
        zllen = ctypes.cast(self.ptr + ZipListHelper.ZL_SIZE_HEADER_SIZE + ZipListHelper.ZL_TAIL_OFFSET_HEADER_SIZE, ctypes.POINTER(ctypes.c_uint32))[0]
        return zlbytes, zltail, zllen

    def _write_header(self, zlbytes: int, zltail: int, zllen: int):
        ctypes.cast(self.ptr, ctypes.POINTER(ctypes.c_uint32))[0] = zlbytes
        ctypes.cast(self.ptr + ZipListHelper.ZL_SIZE_HEADER_SIZE, ctypes.POINTER(ctypes.c_uint32))[0] = zltail
        ctypes.cast(self.ptr + ZipListHelper.ZL_SIZE_HEADER_SIZE + ZipListHelper.ZL_TAIL_OFFSET_HEADER_SIZE, ctypes.POINTER(ctypes.c_uint32))[0] = zllen

    def _first_entry_offset(self) -> int:
        _, _, zllen = self._read_header()
        if zllen == 0:
            return None
        return ZipListHelper.ZL_HEADER_SIZE

    def _next_entry_offset(self, offset: int) -> int:
        if offset is None or offset >= self.size - 1:
            return None
        marker = ctypes.cast(self.ptr + offset, ctypes.POINTER(ctypes.c_uint8))[0]
        if marker == ZipListHelper.END_MARKER:
            return None
            
        _, prev_len_enc_len = ZipListHelper.decode_prev_len(self.ptr, offset)
        _, payload_len = ZipListHelper.decode_entry_payload(self.ptr, offset + prev_len_enc_len)
        
        next_off = offset + prev_len_enc_len + payload_len
        if next_off >= self.size - 1:
            return None
        marker = ctypes.cast(self.ptr + next_off, ctypes.POINTER(ctypes.c_uint8))[0]
        if marker == ZipListHelper.END_MARKER:
            return None
        return next_off

    def _prev_entry_offset(self, offset: int) -> int:
        if offset is None or offset <= ZipListHelper.ZL_HEADER_SIZE:
            return None
        prev_len, _ = ZipListHelper.decode_prev_len(self.ptr, offset)
        if prev_len == 0:
            return None
        return offset - prev_len

    def get_offset_at_index(self, idx: int) -> int:
        _, zltail, zllen = self._read_header()
        if zllen == 0:
            return None
        if idx < 0:
            idx = zllen + idx
        if idx < 0 or idx >= zllen:
            return None
        if idx < zllen // 2:
            curr = ZipListHelper.ZL_HEADER_SIZE
            for _ in range(idx):
                curr = self._next_entry_offset(curr)
            return curr
        else:
            curr = zltail
            for _ in range(zllen - 1 - idx):
                curr = self._prev_entry_offset(curr)
            return curr

    def get_entry_data(self, offset: int) -> bytes:
        if offset is None or offset >= self.size - 1:
            raise IndexError("Offset out of bounds")
        _, prev_len_enc_len = ZipListHelper.decode_prev_len(self.ptr, offset)
        data, _ = ZipListHelper.decode_entry_payload(self.ptr, offset + prev_len_enc_len)
        return data

    def _rebuild(self, elements: list[bytes]):
        entry_blocks = []
        prev_len = 0
        for data in elements:
            enc_prev = ZipListHelper.encode_prev_len(prev_len)
            enc_enc, enc_data = ZipListHelper.encode_entry_payload(data)
            block = enc_prev + enc_enc + enc_data
            entry_blocks.append(block)
            prev_len = len(block)
            
        total_entry_len = sum(len(b) for b in entry_blocks)
        new_size = ZipListHelper.ZL_HEADER_SIZE + total_entry_len + 1
        
        self._resize(new_size)
        
        if len(elements) == 0:
            zltail = ZipListHelper.ZL_HEADER_SIZE
        else:
            zltail = ZipListHelper.ZL_HEADER_SIZE + sum(len(b) for b in entry_blocks[:-1])
            
        self._write_header(new_size, zltail, len(elements))
        
        curr_offset = ZipListHelper.ZL_HEADER_SIZE
        for block in entry_blocks:
            ctypes.memmove(self.ptr + curr_offset, block, len(block))
            curr_offset += len(block)
            
        ctypes.cast(self.ptr + curr_offset, ctypes.POINTER(ctypes.c_uint8))[0] = ZipListHelper.END_MARKER

    def insert_at_offset(self, offset: int, data: bytes):
        if isinstance(data, str):
            data = data.encode()
            
        zlbytes, _, zllen = self._read_header()
        if offset == ZipListHelper.ZL_HEADER_SIZE:
            idx = 0
        elif offset == zlbytes - 1:
            idx = zllen
        else:
            idx = None
            curr = ZipListHelper.ZL_HEADER_SIZE
            for i in range(zllen):
                if curr == offset:
                    idx = i
                    break
                curr = self._next_entry_offset(curr)
            if idx is None:
                raise IndexError("Offset not found in ziplist")
                
        elements = list(self)
        elements.insert(idx, data)
        self._rebuild(elements)

    def delete_at_offset(self, offset: int):
        zlbytes, _, zllen = self._read_header()
        if zllen == 0 or offset is None or offset >= zlbytes - 1:
            return
            
        idx = None
        curr = ZipListHelper.ZL_HEADER_SIZE
        for i in range(zllen):
            if curr == offset:
                idx = i
                break
            curr = self._next_entry_offset(curr)
        if idx is None:
            return
            
        elements = list(self)
        elements.pop(idx)
        self._rebuild(elements)

    def lpush(self, data: bytes):
        self.insert_at_offset(ZipListHelper.ZL_HEADER_SIZE, data)

    def rpush(self, data: bytes):
        zlbytes, _, _ = self._read_header()
        self.insert_at_offset(zlbytes - 1, data)

    def lpop(self) -> bytes:
        if len(self) == 0:
            return None
        offset = ZipListHelper.ZL_HEADER_SIZE
        data = self.get_entry_data(offset)
        self.delete_at_offset(offset)
        return data

    def rpop(self) -> bytes:
        _, zltail, zllen = self._read_header()
        if zllen == 0:
            return None
        data = self.get_entry_data(zltail)
        self.delete_at_offset(zltail)
        return data

    def __len__(self) -> int:
        _, _, zllen = self._read_header()
        return zllen

    def __iter__(self):
        curr = self._first_entry_offset()
        while curr is not None:
            yield self.get_entry_data(curr)
            curr = self._next_entry_offset(curr)

    def __getitem__(self, idx: int) -> bytes:
        offset = self.get_offset_at_index(idx)
        if offset is None:
            raise IndexError("Index out of range")
        return self.get_entry_data(offset)


class QuickListNodeStruct(ctypes.Structure):
    _fields_ = [
        ("prev", ctypes.c_void_p),
        ("next", ctypes.c_void_p),
        ("zl", ctypes.c_void_p),
        ("sz", ctypes.c_size_t),
        ("count", ctypes.c_uint32)
    ]


class QuickListStruct(ctypes.Structure):
    _fields_ = [
        ("head", ctypes.c_void_p),
        ("tail", ctypes.c_void_p),
        ("count", ctypes.c_size_t),
        ("len", ctypes.c_size_t)
    ]


class QuickList:
    def __init__(self, max_entries: int = 512, max_bytes: int = 8192, ptr=None):
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        if ptr is not None:
            # Viewing an existing structure in memory (no ownership/free responsibilities)
            self.struct_addr = ptr
            self.has_ownership = False
        else:
            # Allocate a new QuickListStruct in C memory
            self._struct_ptr = Malloc.alloc_struct(QuickListStruct)
            self.struct_addr = self._struct_ptr.ptr
            self.has_ownership = True
            
            struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
            struct.head = None
            struct.tail = None
            struct.count = 0
            struct.len = 0
            
            # Register weakref finalizer for automatic garbage collection cleanup
            self._finalizer = weakref.finalize(
                self,
                self._cleanup,
                self._struct_ptr
            )

    @staticmethod
    def _cleanup(struct_ptr: MallocInternal) -> None:
        if struct_ptr and struct_ptr.ptr:
            struct = ctypes.cast(struct_ptr.ptr, ctypes.POINTER(QuickListStruct)).contents
            curr_ptr = struct.head
            while curr_ptr:
                curr_node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                next_ptr = curr_node.next
                if curr_node.zl:
                    MallocInternal.zfree(curr_node.zl) # Free node's ziplist
                MallocInternal.zfree(curr_ptr) # Free node struct itself
                curr_ptr = next_ptr
            struct_ptr.free() # Free base list struct

    def free(self) -> None:
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer()

    def release(self) -> int:
        self.has_ownership = False
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
        if hasattr(self, "_struct_ptr") and hasattr(self._struct_ptr, "_finalizer") and self._struct_ptr._finalizer.alive:
            self._struct_ptr._finalizer.detach()
        return self.struct_addr

    def __len__(self) -> int:
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        return struct.count

    @property
    def node_count(self) -> int:
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        return struct.len

    def rpush(self, data: bytes):
        if isinstance(data, str):
            data = data.encode()
            
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        
        if struct.len == 0:
            # Create first node
            node_ptr = MallocInternal.zcalloc(ctypes.sizeof(QuickListNodeStruct))
            node = ctypes.cast(node_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            zl = ZipList()
            zl.rpush(data)
            
            node.zl = zl.release()
            node.sz = zl.size
            node.count = len(zl)
            node.prev = None
            node.next = None
            
            struct.head = node_ptr
            struct.tail = node_ptr
            struct.len = 1
            struct.count = 1
        else:
            # Check tail node
            tail_ptr = struct.tail
            tail_node = ctypes.cast(tail_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            zl = ZipList(ptr=tail_node.zl)
            
            if len(zl) < self.max_entries and zl.size + len(data) + 8 <= self.max_bytes:
                zl.rpush(data)
                tail_node.zl = zl.ptr
                tail_node.sz = zl.size
                tail_node.count = len(zl)
                struct.count += 1
            else:
                # Tail node is full, create a new tail node
                node_ptr = MallocInternal.zcalloc(ctypes.sizeof(QuickListNodeStruct))
                node = ctypes.cast(node_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                zl_new = ZipList()
                zl_new.rpush(data)
                
                node.zl = zl_new.release()
                node.sz = zl_new.size
                node.count = len(zl_new)
                node.prev = tail_ptr
                node.next = None
                
                tail_node.next = node_ptr
                struct.tail = node_ptr
                struct.len += 1
                struct.count += 1

    def lpush(self, data: bytes):
        if isinstance(data, str):
            data = data.encode()
            
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        
        if struct.len == 0:
            # Create first node
            node_ptr = MallocInternal.zcalloc(ctypes.sizeof(QuickListNodeStruct))
            node = ctypes.cast(node_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            zl = ZipList()
            zl.lpush(data)
            
            node.zl = zl.release()
            node.sz = zl.size
            node.count = len(zl)
            node.prev = None
            node.next = None
            
            struct.head = node_ptr
            struct.tail = node_ptr
            struct.len = 1
            struct.count = 1
        else:
            # Check head node
            head_ptr = struct.head
            head_node = ctypes.cast(head_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            zl = ZipList(ptr=head_node.zl)
            
            if len(zl) < self.max_entries and zl.size + len(data) + 8 <= self.max_bytes:
                zl.lpush(data)
                head_node.zl = zl.ptr
                head_node.sz = zl.size
                head_node.count = len(zl)
                struct.count += 1
            else:
                # Head node is full, create a new head node
                node_ptr = MallocInternal.zcalloc(ctypes.sizeof(QuickListNodeStruct))
                node = ctypes.cast(node_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                zl_new = ZipList()
                zl_new.lpush(data)
                
                node.zl = zl_new.release()
                node.sz = zl_new.size
                node.count = len(zl_new)
                node.prev = None
                node.next = head_ptr
                
                head_node.prev = node_ptr
                struct.head = node_ptr
                struct.len += 1
                struct.count += 1

    def lpop(self) -> bytes:
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        if struct.count == 0:
            return None
            
        head_ptr = struct.head
        head_node = ctypes.cast(head_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
        zl = ZipList(ptr=head_node.zl)
        
        data = zl.lpop()
        head_node.zl = zl.ptr
        head_node.sz = zl.size
        head_node.count = len(zl)
        struct.count -= 1
        
        if len(zl) == 0:
            next_ptr = head_node.next
            if next_ptr:
                next_node = ctypes.cast(next_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                next_node.prev = None
                struct.head = next_ptr
            else:
                struct.head = None
                struct.tail = None
            MallocInternal.zfree(head_node.zl)
            MallocInternal.zfree(head_ptr)
            struct.len -= 1
            
        return data

    def rpop(self) -> bytes:
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        if struct.count == 0:
            return None
            
        tail_ptr = struct.tail
        tail_node = ctypes.cast(tail_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
        zl = ZipList(ptr=tail_node.zl)
        
        data = zl.rpop()
        tail_node.zl = zl.ptr
        tail_node.sz = zl.size
        tail_node.count = len(zl)
        struct.count -= 1
        
        if len(zl) == 0:
            prev_ptr = tail_node.prev
            if prev_ptr:
                prev_node = ctypes.cast(prev_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                prev_node.next = None
                struct.tail = prev_ptr
            else:
                struct.head = None
                struct.tail = None
            MallocInternal.zfree(tail_node.zl)
            MallocInternal.zfree(tail_ptr)
            struct.len -= 1
            
        return data

    def __getitem__(self, idx: int) -> bytes:
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        total = struct.count
        if idx < 0:
            idx = total + idx
        if idx < 0 or idx >= total:
            raise IndexError("Index out of range")
            
        curr_ptr = struct.head
        while curr_ptr:
            node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            if idx < node.count:
                zl = ZipList(ptr=node.zl)
                return zl[idx]
            idx -= node.count
            curr_ptr = node.next
        raise IndexError("Index out of range")

    def __iter__(self):
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        curr_ptr = struct.head
        while curr_ptr:
            node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            zl = ZipList(ptr=node.zl)
            for item in zl:
                yield item
            curr_ptr = node.next

    def insert(self, idx: int, data: bytes):
        if isinstance(data, str):
            data = data.encode()
            
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        total = struct.count
        
        if idx < 0:
            idx = total + idx
            
        if idx <= 0:
            self.lpush(data)
            return
            
        if idx >= total:
            self.rpush(data)
            return
            
        # Traverse to find the node and relative index
        curr_ptr = struct.head
        rel_idx = idx
        while curr_ptr:
            node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            if rel_idx < node.count:
                zl = ZipList(ptr=node.zl)
                if len(zl) < self.max_entries and zl.size + len(data) + 8 <= self.max_bytes:
                    offset = zl.get_offset_at_index(rel_idx)
                    zl.insert_at_offset(offset, data)
                    node.zl = zl.ptr
                    node.sz = zl.size
                    node.count = len(zl)
                    struct.count += 1
                else:
                    # Node is full, we must split and insert
                    elements = list(zl)
                    left = elements[:rel_idx]
                    right = elements[rel_idx:]
                    left.append(data)
                    
                    # Create two new ZipLists
                    zl_left = ZipList()
                    for x in left:
                        zl_left.rpush(x)
                        
                    zl_right = ZipList()
                    for x in right:
                        zl_right.rpush(x)
                        
                    # Replace current node's ziplist
                    MallocInternal.zfree(node.zl)
                    node.zl = zl_left.release()
                    node.sz = zl_left.size
                    node.count = len(zl_left)
                    
                    # Create new node for right part
                    new_node_ptr = MallocInternal.zcalloc(ctypes.sizeof(QuickListNodeStruct))
                    new_node = ctypes.cast(new_node_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                    new_node.zl = zl_right.release()
                    new_node.sz = zl_right.size
                    new_node.count = len(zl_right)
                    
                    # Insert new node in doubly linked list
                    next_ptr = node.next
                    new_node.prev = curr_ptr
                    new_node.next = next_ptr
                    node.next = new_node_ptr
                    
                    if next_ptr:
                        next_node = ctypes.cast(next_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                        next_node.prev = new_node_ptr
                    else:
                        struct.tail = new_node_ptr
                        
                    struct.len += 1
                    struct.count += 1
                return
            rel_idx -= node.count
            curr_ptr = node.next

    def delete(self, idx: int):
        struct = ctypes.cast(self.struct_addr, ctypes.POINTER(QuickListStruct)).contents
        total = struct.count
        if idx < 0:
            idx = total + idx
        if idx < 0 or idx >= total:
            raise IndexError("Index out of range")
            
        curr_ptr = struct.head
        rel_idx = idx
        while curr_ptr:
            node = ctypes.cast(curr_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
            if rel_idx < node.count:
                zl = ZipList(ptr=node.zl)
                offset = zl.get_offset_at_index(rel_idx)
                zl.delete_at_offset(offset)
                
                node.zl = zl.ptr
                node.sz = zl.size
                node.count = len(zl)
                struct.count -= 1
                
                if len(zl) == 0:
                    prev_ptr = node.prev
                    next_ptr = node.next
                    
                    if prev_ptr:
                        prev_node = ctypes.cast(prev_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                        prev_node.next = next_ptr
                    else:
                        struct.head = next_ptr
                        
                    if next_ptr:
                        next_node = ctypes.cast(next_ptr, ctypes.POINTER(QuickListNodeStruct)).contents
                        next_node.prev = prev_ptr
                    else:
                        struct.tail = prev_ptr
                        
                    MallocInternal.zfree(node.zl)
                    MallocInternal.zfree(curr_ptr)
                    struct.len -= 1
                return
            rel_idx -= node.count
            curr_ptr = node.next
