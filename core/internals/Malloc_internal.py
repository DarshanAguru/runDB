import ctypes 
from ctypes import util
import weakref
import logging

logger = logging.getLogger(__name__)

allocator_name = "libc"

# Attempt to load jemalloc from local dll directory
try:
    libc = ctypes.CDLL("./dll/libjemalloc.so")
    logger.debug("Successfully loaded jemalloc from ./dll/libjemalloc.so for native allocations!")
    allocator_name = "jemalloc"
except OSError:
    # Fallback to standard C library
    # Note: if the server was started with LD_PRELOAD=./dll/libjemalloc.so,
    # the standard libc malloc/free will transparently use jemalloc.
    libc = ctypes.CDLL(util.find_library("c"))
    logger.debug("Successfully loaded libc for native allocations!")
    allocator_name = "libc"

libc.malloc.argtypes = [ctypes.c_size_t]
libc.malloc.restype = ctypes.c_void_p
libc.realloc.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
libc.realloc.restype = ctypes.c_void_p
libc.free.argtypes = [ctypes.c_void_p]
libc.free.restype = None

PREFIX_SIZE = ctypes.sizeof(ctypes.c_size_t)


class MemTracker:
    allocated = 0
    blocks = 0

    # Tracks memory allocation by increasing allocated bytes and block count
    @classmethod
    def alloc(cls, size):
        cls.allocated += size
        cls.blocks += 1
    
    # Tracks memory deallocation by decreasing allocated bytes and block count
    @classmethod
    def free(cls, size):
        cls.allocated -= size
        cls.blocks -= 1
    
    # Returns the current memory tracking statistics
    @classmethod
    def stats(cls):
        return {
            "bytes": cls.allocated,
            "blocks": cls.blocks
        }


class MallocInternal:

    __slots__ = ("ptr", "size", "_finalizer",  "__weakref__")

    # Allocates native C memory using zmalloc and registers a finalizer for tracking
    def __init__(self, size: int):
        self.size = size
        self.ptr = self.zmalloc(size)

        self._finalizer = weakref.finalize(
            self,
            self._cleanup,
            self.ptr
        )
    
    # Frees the native pointer using zfree
    @staticmethod
    def _cleanup(ptr):
        MallocInternal.zfree(ptr)

    @staticmethod
    def zmalloc(size: int) -> int:
        if size < 0:
            raise ValueError("Allocation size cannot be negative")
        raw_ptr = libc.malloc(size + PREFIX_SIZE)
        if not raw_ptr:
            raise MemoryError(f"Failed to allocate native memory block of {size} bytes (Out of Memory).")
        
        ctypes.cast(raw_ptr, ctypes.POINTER(ctypes.c_size_t))[0] = size
        MemTracker.alloc(size)
        return raw_ptr + PREFIX_SIZE

    @staticmethod
    def zcalloc(size: int) -> int:
        ptr = MallocInternal.zmalloc(size)
        ctypes.memset(ptr, 0, size)
        return ptr

    @staticmethod
    def zrealloc(ptr, size: int) -> int:
        if size < 0:
            raise ValueError("Allocation size cannot be negative")
        if ptr is None or ptr == 0:
            return MallocInternal.zmalloc(size)
        if size == 0:
            MallocInternal.zfree(ptr)
            return None
        
        addr = ctypes.cast(ptr, ctypes.c_void_p).value
        if not addr:
            return MallocInternal.zmalloc(size)
            
        raw_ptr = addr - PREFIX_SIZE
        old_size = ctypes.cast(raw_ptr, ctypes.POINTER(ctypes.c_size_t))[0]
        
        new_raw_ptr = libc.realloc(raw_ptr, size + PREFIX_SIZE)
        if not new_raw_ptr:
            raise MemoryError(f"Failed to reallocate native memory block of {size} bytes (Out of Memory).")
        
        ctypes.cast(new_raw_ptr, ctypes.POINTER(ctypes.c_size_t))[0] = size
        MemTracker.allocated += (size - old_size)
        return new_raw_ptr + PREFIX_SIZE

    @staticmethod
    def zfree(ptr) -> None:
        addr = ctypes.cast(ptr, ctypes.c_void_p).value
        if not addr:
            return
        raw_ptr = addr - PREFIX_SIZE
        size = ctypes.cast(raw_ptr, ctypes.POINTER(ctypes.c_size_t))[0]
        libc.free(raw_ptr)
        MemTracker.free(size)

    @staticmethod
    def zmalloc_usable_size(ptr) -> int:
        addr = ctypes.cast(ptr, ctypes.c_void_p).value
        if not addr:
            return 0
        raw_ptr = addr - PREFIX_SIZE
        return ctypes.cast(raw_ptr, ctypes.POINTER(ctypes.c_size_t))[0]

    @staticmethod
    def zmalloc_used_memory() -> int:
        return MemTracker.allocated

    @staticmethod
    def zmalloc_get_allocator() -> str:
        return allocator_name

    # Writes raw bytes to the allocated native C memory
    def write(self, data: bytes):
        if len(data) > self.size:
            raise self.value_error()
        ctypes.memmove(self.ptr, data, len(data))

    # Reads and decodes a null-terminated string from C memory
    def read_string(self):
        return ctypes.string_at(
            self.ptr
        ).decode()


    # Reads a specified number of raw bytes from native C memory
    def read_bytes(self, size=None):
        n = self.size if size is None else min(
            size,
            self.size
        )

        return ctypes.string_at(
            self.ptr,
            n
        )


    # Reads a ctypes type value directly from native C memory
    def read_type(self, ctype):
        return self.as_type(ctype)[0]
    
    # Explicitly triggers the finalizer to free the C memory and update stats
    def free(self):
        if self._finalizer.alive:
            self._finalizer()
    
    # Casts the raw C pointer to a POINTER of the specified ctypes type
    def as_type(self, ctype):
        return ctypes.cast(
            self.ptr,
            ctypes.POINTER(ctype)
        )
    
    # Raises a ValueError indicating insufficient memory to allocate
    def value_error(self):
        return ValueError(f"Data size exceeds the allocated buffer capacity of {self.size} bytes.")

    # Raises a MemoryError indicating memory overflow
    def oom_error(self):
        return MemoryError(f"Failed to allocate native memory block of {self.size} bytes (Out of Memory).")
    
