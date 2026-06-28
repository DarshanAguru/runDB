import ctypes
import weakref
from typing import Any
from .Malloc_internal import MallocInternal

# SDS Header Types
SDS_TYPE_5 = 0
SDS_TYPE_8 = 1
SDS_TYPE_16 = 2
SDS_TYPE_32 = 3
SDS_TYPE_64 = 4

class sdshdr5(ctypes.Structure):
    _packed_ = True
    _fields_ = [
        ("flags", ctypes.c_uint8),
    ]

class sdshdr8(ctypes.Structure):
    _packed_ = True
    _fields_ = [
        ("len", ctypes.c_uint8),
        ("alloc", ctypes.c_uint8),
        ("flags", ctypes.c_uint8),
    ]

class sdshdr16(ctypes.Structure):
    _packed_ = True
    _fields_ = [
        ("len", ctypes.c_uint16),
        ("alloc", ctypes.c_uint16),
        ("flags", ctypes.c_uint8),
    ]

class sdshdr32(ctypes.Structure):
    _packed_ = True
    _fields_ = [
        ("len", ctypes.c_uint32),
        ("alloc", ctypes.c_uint32),
        ("flags", ctypes.c_uint8),
    ]

class sdshdr64(ctypes.Structure):
    _packed_ = True
    _fields_ = [
        ("len", ctypes.c_uint64),
        ("alloc", ctypes.c_uint64),
        ("flags", ctypes.c_uint8),
    ]


class sdsHelpers:
    @staticmethod
    def sdsReqType(string_size: int) -> int:
        if string_size < 32:
            return SDS_TYPE_5
        if string_size < 0xFF:
            return SDS_TYPE_8
        if string_size < 0xFFFF:
            return SDS_TYPE_16
        if string_size < 0xFFFFFFFF:
            return SDS_TYPE_32
        return SDS_TYPE_64

    @staticmethod
    def sdsHdrSize(sds_type: int) -> int:
        if sds_type == SDS_TYPE_5:
            return 1
        if sds_type == SDS_TYPE_8:
            return 3
        if sds_type == SDS_TYPE_16:
            return 5
        if sds_type == SDS_TYPE_32:
            return 9
        if sds_type == SDS_TYPE_64:
            return 17
        raise ValueError(f"Unknown sds type: {sds_type}")

    @staticmethod
    def sds_get_type(sds_ptr: int) -> int:
        flags = ctypes.cast(sds_ptr - 1, ctypes.POINTER(ctypes.c_uint8))[0]
        return flags & 7

    @staticmethod
    def sdslen(sds_ptr: int) -> int:
        if not sds_ptr:
            return 0
        t = sdsHelpers.sds_get_type(sds_ptr)
        hdr_size = sdsHelpers.sdsHdrSize(t)
        hdr_ptr = sds_ptr - hdr_size
        if t == SDS_TYPE_5:
            flags = ctypes.cast(sds_ptr - 1, ctypes.POINTER(ctypes.c_uint8))[0]
            return flags >> 3
        elif t == SDS_TYPE_8:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr8)).contents.len
        elif t == SDS_TYPE_16:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr16)).contents.len
        elif t == SDS_TYPE_32:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr32)).contents.len
        elif t == SDS_TYPE_64:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr64)).contents.len
        return 0

    @staticmethod
    def sdssetlen(sds_ptr: int, newlen: int) -> None:
        t = sdsHelpers.sds_get_type(sds_ptr)
        hdr_size = sdsHelpers.sdsHdrSize(t)
        hdr_ptr = sds_ptr - hdr_size
        if t == SDS_TYPE_5:
            flags_ptr = ctypes.cast(sds_ptr - 1, ctypes.POINTER(ctypes.c_uint8))
            flags_ptr[0] = (newlen << 3) | SDS_TYPE_5
        elif t == SDS_TYPE_8:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr8)).contents.len = newlen
        elif t == SDS_TYPE_16:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr16)).contents.len = newlen
        elif t == SDS_TYPE_32:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr32)).contents.len = newlen
        elif t == SDS_TYPE_64:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr64)).contents.len = newlen

    @staticmethod
    def sdsalloc(sds_ptr: int) -> int:
        if not sds_ptr:
            return 0
        t = sdsHelpers.sds_get_type(sds_ptr)
        hdr_size = sdsHelpers.sdsHdrSize(t)
        hdr_ptr = sds_ptr - hdr_size
        if t == SDS_TYPE_5:
            flags = ctypes.cast(sds_ptr - 1, ctypes.POINTER(ctypes.c_uint8))[0]
            return flags >> 3
        elif t == SDS_TYPE_8:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr8)).contents.alloc
        elif t == SDS_TYPE_16:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr16)).contents.alloc
        elif t == SDS_TYPE_32:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr32)).contents.alloc
        elif t == SDS_TYPE_64:
            return ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr64)).contents.alloc
        return 0

    @staticmethod
    def sdssetalloc(sds_ptr: int, newalloc: int) -> None:
        t = sdsHelpers.sds_get_type(sds_ptr)
        hdr_size = sdsHelpers.sdsHdrSize(t)
        hdr_ptr = sds_ptr - hdr_size
        if t == SDS_TYPE_5:
            pass
        elif t == SDS_TYPE_8:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr8)).contents.alloc = newalloc
        elif t == SDS_TYPE_16:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr16)).contents.alloc = newalloc
        elif t == SDS_TYPE_32:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr32)).contents.alloc = newalloc
        elif t == SDS_TYPE_64:
            ctypes.cast(hdr_ptr, ctypes.POINTER(sdshdr64)).contents.alloc = newalloc

    @staticmethod
    def sdsavail(sds_ptr: int) -> int:
        if not sds_ptr:
            return 0
        t = sdsHelpers.sds_get_type(sds_ptr)
        if t == SDS_TYPE_5:
            return 0
        return sdsHelpers.sdsalloc(sds_ptr) - sdsHelpers.sdslen(sds_ptr)

    @staticmethod
    def sdsnewlen(init: bytes = None, initlen: int = 0) -> int:
        t = sdsHelpers.sdsReqType(initlen)
        if t == SDS_TYPE_5 and initlen == 0:
            t = SDS_TYPE_8
            
        hdr_size = sdsHelpers.sdsHdrSize(t)
        raw_ptr = MallocInternal.zmalloc(hdr_size + initlen + 1)
        sds_ptr = raw_ptr + hdr_size
        
        if t == SDS_TYPE_5:
            flags_ptr = ctypes.cast(sds_ptr - 1, ctypes.POINTER(ctypes.c_uint8))
            flags_ptr[0] = (initlen << 3) | SDS_TYPE_5
        elif t == SDS_TYPE_8:
            hdr = ctypes.cast(raw_ptr, ctypes.POINTER(sdshdr8)).contents
            hdr.len = initlen
            hdr.alloc = initlen
            hdr.flags = SDS_TYPE_8
        elif t == SDS_TYPE_16:
            hdr = ctypes.cast(raw_ptr, ctypes.POINTER(sdshdr16)).contents
            hdr.len = initlen
            hdr.alloc = initlen
            hdr.flags = SDS_TYPE_16
        elif t == SDS_TYPE_32:
            hdr = ctypes.cast(raw_ptr, ctypes.POINTER(sdshdr32)).contents
            hdr.len = initlen
            hdr.alloc = initlen
            hdr.flags = SDS_TYPE_32
        elif t == SDS_TYPE_64:
            hdr = ctypes.cast(raw_ptr, ctypes.POINTER(sdshdr64)).contents
            hdr.len = initlen
            hdr.alloc = initlen
            hdr.flags = SDS_TYPE_64
            
        if init:
            ctypes.memmove(sds_ptr, init, initlen)
        ctypes.cast(sds_ptr + initlen, ctypes.POINTER(ctypes.c_char))[0] = b"\0"
        return sds_ptr

    @staticmethod
    def sdsnew(init_val: str | bytes = b"") -> int:
        if isinstance(init_val, str):
            init_bytes = init_val.encode('utf-8')
        else:
            init_bytes = init_val
        return sdsHelpers.sdsnewlen(init_bytes, len(init_bytes))

    @staticmethod
    def sdsfree(sds_ptr: int) -> None:
        if not sds_ptr:
            return
        t = sdsHelpers.sds_get_type(sds_ptr)
        hdr_size = sdsHelpers.sdsHdrSize(t)
        raw_ptr = sds_ptr - hdr_size
        MallocInternal.zfree(raw_ptr)

    @staticmethod
    def sdsMakeRoomFor(sds_ptr: int, addlen: int) -> int:
        if not sds_ptr:
            return 0
            
        avail = sdsHelpers.sdsavail(sds_ptr)
        if avail >= addlen:
            return sds_ptr
            
        old_len = sdsHelpers.sdslen(sds_ptr)
        new_len = old_len + addlen
        
        if new_len < 1024 * 1024:
            new_alloc = new_len * 2
        else:
            new_alloc = new_len + 1024 * 1024
            
        t = sdsHelpers.sds_get_type(sds_ptr)
        old_hdr_size = sdsHelpers.sdsHdrSize(t)
        
        new_type = sdsHelpers.sdsReqType(new_alloc)
        if new_type == SDS_TYPE_5:
            new_type = SDS_TYPE_8
            
        new_hdr_size = sdsHelpers.sdsHdrSize(new_type)
        old_raw_ptr = sds_ptr - old_hdr_size
        
        if t == new_type:
            new_raw_ptr = MallocInternal.zrealloc(old_raw_ptr, old_hdr_size + new_alloc + 1)
            sds_ptr = new_raw_ptr + old_hdr_size
            sdsHelpers.sdssetalloc(sds_ptr, new_alloc)
        else:
            new_raw_ptr = MallocInternal.zmalloc(new_hdr_size + new_alloc + 1)
            new_sds_ptr = new_raw_ptr + new_hdr_size
            ctypes.memmove(new_sds_ptr, sds_ptr, old_len)
            MallocInternal.zfree(old_raw_ptr)
            sds_ptr = new_sds_ptr
            
            if new_type == SDS_TYPE_8:
                hdr = ctypes.cast(new_raw_ptr, ctypes.POINTER(sdshdr8)).contents
                hdr.len = old_len
                hdr.alloc = new_alloc
                hdr.flags = SDS_TYPE_8
            elif new_type == SDS_TYPE_16:
                hdr = ctypes.cast(new_raw_ptr, ctypes.POINTER(sdshdr16)).contents
                hdr.len = old_len
                hdr.alloc = new_alloc
                hdr.flags = SDS_TYPE_16
            elif new_type == SDS_TYPE_32:
                hdr = ctypes.cast(new_raw_ptr, ctypes.POINTER(sdshdr32)).contents
                hdr.len = old_len
                hdr.alloc = new_alloc
                hdr.flags = SDS_TYPE_32
            elif new_type == SDS_TYPE_64:
                hdr = ctypes.cast(new_raw_ptr, ctypes.POINTER(sdshdr64)).contents
                hdr.len = old_len
                hdr.alloc = new_alloc
                hdr.flags = SDS_TYPE_64
                
        return sds_ptr

    @staticmethod
    def sdscatlen(sds_ptr: int, t: bytes, t_len: int) -> int:
        sds_ptr = sdsHelpers.sdsMakeRoomFor(sds_ptr, t_len)
        curr_len = sdsHelpers.sdslen(sds_ptr)
        ctypes.memmove(sds_ptr + curr_len, t, t_len)
        new_len = curr_len + t_len
        sdsHelpers.sdssetlen(sds_ptr, new_len)
        ctypes.cast(sds_ptr + new_len, ctypes.POINTER(ctypes.c_char))[0] = b"\0"
        return sds_ptr

    @staticmethod
    def sdscat(sds_ptr: int, t: str | bytes) -> int:
        if isinstance(t, str):
            t_bytes = t.encode('utf-8')
        else:
            t_bytes = t
        return sdsHelpers.sdscatlen(sds_ptr, t_bytes, len(t_bytes))

    @staticmethod
    def sdscpy(sds_ptr: int, t: bytes) -> int:
        t_len = len(t)
        curr_alloc = sdsHelpers.sdsalloc(sds_ptr)
        if curr_alloc < t_len:
            sds_ptr = sdsHelpers.sdsMakeRoomFor(sds_ptr, t_len - sdsHelpers.sdslen(sds_ptr))
        ctypes.memmove(sds_ptr, t, t_len)
        sdsHelpers.sdssetlen(sds_ptr, t_len)
        ctypes.cast(sds_ptr + t_len, ctypes.POINTER(ctypes.c_char))[0] = b"\0"
        return sds_ptr

    @staticmethod
    def sdsrepr(sds_ptr: int) -> bytes:
        if not sds_ptr:
            return b""
        length = sdsHelpers.sdslen(sds_ptr)
        return ctypes.string_at(sds_ptr, length)






class SDSPtr:
    __slots__ = ["ptr"]
    def __init__(self, ptr: int):
        self.ptr = ptr


class SDS:
    __slots__ = ["_sds_ptr", "_finalizer", "__weakref__"]
    
    def __init__(self, init_val: str | bytes = b"", ptr: int = None):
        if ptr is not None:
            self._sds_ptr = SDSPtr(ptr)
            self._finalizer = None
        else:
            self._sds_ptr = SDSPtr(sdsHelpers.sdsnew(init_val))
            self._finalizer = weakref.finalize(self, self._cleanup, self._sds_ptr)
            
    @staticmethod
    def _cleanup(sds_ptr_obj: SDSPtr) -> None:
        if sds_ptr_obj and sds_ptr_obj.ptr:
            sdsHelpers.sdsfree(sds_ptr_obj.ptr)
            sds_ptr_obj.ptr = None
            
    def release(self) -> int:
        if hasattr(self, "_finalizer") and self._finalizer and self._finalizer.alive:
            self._finalizer.detach()
        ptr = self._sds_ptr.ptr
        self._sds_ptr.ptr = None
        return ptr
        
    def free(self) -> None:
        if hasattr(self, "_finalizer") and self._finalizer and self._finalizer.alive:
            self._finalizer()
        else:
            self._cleanup(self._sds_ptr)
            
    def __len__(self) -> int:
        return sdsHelpers.sdslen(self._sds_ptr.ptr)
        
    @property
    def avail(self) -> int:
        return sdsHelpers.sdsavail(self._sds_ptr.ptr)
        
    @property
    def alloc(self) -> int:
        return sdsHelpers.sdsalloc(self._sds_ptr.ptr)
        
    def append(self, t: str | bytes) -> None:
        self._sds_ptr.ptr = sdsHelpers.sdscat(self._sds_ptr.ptr, t)
        
    def copy(self, t: bytes) -> None:
        self._sds_ptr.ptr = sdsHelpers.sdscpy(self._sds_ptr.ptr, t)
        
    def __str__(self) -> str:
        return sdsHelpers.sdsrepr(self._sds_ptr.ptr).decode('utf-8', errors='replace')
        
    def __bytes__(self) -> bytes:
        return sdsHelpers.sdsrepr(self._sds_ptr.ptr)
