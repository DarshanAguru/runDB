import ctypes 
from ctypes import util
import weakref
import logging

logger = logging.getLogger(__name__)

libc = None

# Attempt to load jemalloc from local dll directory
try:
    libc = ctypes.CDLL("./dll/libjemalloc.so")
    logger.debug("Successfully loaded jemalloc from ./dll/libjemalloc.so for native allocations!")
except OSError:
    # Fallback to standard C library
    # Note: if the server was started with LD_PRELOAD=./dll/libjemalloc.so,
    # the standard libc malloc/free will transparently use jemalloc.
    libc = ctypes.CDLL(util.find_library("c"))

libc.malloc.argtypes = [ctypes.c_size_t]
libc.malloc.restype = ctypes.c_void_p
libc.free.argtypes = [ctypes.c_void_p]
libc.free.restype = None


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

    # Allocates native C memory using libc malloc and registers a finalizer for tracking
    def __init__(self, size: int):
        self.size = size
        self.ptr = libc.malloc(size)

        if not self.ptr:
            raise self.oom_error()
        
        MemTracker.alloc(size)

        self._finalizer = weakref.finalize(
            self,
            self._cleanup,
            self.ptr,
            self.size
        )
    
    # Frees the native pointer and updates the memory tracker stats
    @staticmethod
    def _cleanup(ptr, size):
        libc.free(ptr)
        MemTracker.free(size)

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
    
