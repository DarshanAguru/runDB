import ctypes
import weakref
from typing import Any
from config import Config
from .Intset import Intset
from .HashTable import HashTable

# Encoding constants
OBJ_ENCODING_INTSET = 11
OBJ_ENCODING_HT = 12

SET_MAX_INTSET_ENTRIES = getattr(Config, "SET_MAX_INTSET_ENTRIES", 512)

# Set orchestrates a polymorphic, self-upgrading Set container:
# - Encoding Promotion: Starts as a space-efficient Intset representation.
# - Auto-Conversion: Automatically upgrades to a HashTable when member count exceeds 
#   SET_MAX_INTSET_ENTRIES or a non-integer member is added.
# - C-Heap Handover: Detaches child finalizers during transition to prevent premature memory release.
class Set:
    def __init__(self, ptr=None, encoding=None):
        if encoding is None:
            encoding = OBJ_ENCODING_INTSET
            
        self.encoding = encoding
        self.has_ownership = True
        
        if ptr is not None:
            self.has_ownership = False
            self.ptr = ptr
            if self.encoding == OBJ_ENCODING_INTSET:
                self.underlying = Intset(ptr=ptr)
            else:
                self.underlying = HashTable("string", ptr=ptr)
        else:
            if self.encoding == OBJ_ENCODING_INTSET:
                self.underlying = Intset()
                self.ptr = self.underlying.ptr
            else:
                self.underlying = HashTable("string")
                self.ptr = self.underlying.release()
                
            self._finalizer = weakref.finalize(
                self,
                self._cleanup,
                self.ptr,
                self.encoding
            )

    @staticmethod
    def _cleanup(ptr, encoding):
        if not ptr:
            return
        if encoding == OBJ_ENCODING_INTSET:
            from .Malloc_internal import MallocInternal
            MallocInternal.zfree(ptr)
        else:
            from .HashTable import HashTable
            ht = HashTable("string", ptr=ptr)
            ht.map.has_ownership = True
            ht.map.free()

    def release(self) -> int:
        self.has_ownership = False
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
            
        if self.encoding == OBJ_ENCODING_INTSET:
            return self.underlying.release()
        else:
            return self.underlying.release()

    def add(self, member: Any) -> bool:
        if self.encoding == OBJ_ENCODING_INTSET:
            is_int = isinstance(member, int) and not isinstance(member, bool)
            if not is_int or len(self.underlying) >= SET_MAX_INTSET_ENTRIES:
                self._convert_to_ht()
            else:
                return self.underlying.add(member)
                
        return self.underlying.add(str(member))

    def _convert_to_ht(self):
        new_ht = HashTable("string")
        for elem in self.underlying:
            new_ht.add(str(elem))
            
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
            
        self.underlying.has_ownership = True
        self.underlying.free()
        
        self.encoding = OBJ_ENCODING_HT
        self.underlying = new_ht
        self.ptr = new_ht.release()
        
        self._finalizer = weakref.finalize(
            self,
            self._cleanup,
            self.ptr,
            self.encoding
        )

    def remove(self, member: Any) -> bool:
        if self.encoding == OBJ_ENCODING_INTSET:
            if not isinstance(member, int) or isinstance(member, bool):
                return False
            return self.underlying.remove(member)
        else:
            return self.underlying.remove(str(member))

    def __contains__(self, member: Any) -> bool:
        if self.encoding == OBJ_ENCODING_INTSET:
            if not isinstance(member, int) or isinstance(member, bool):
                return False
            return member in self.underlying
        else:
            return str(member) in self.underlying

    def __len__(self) -> int:
        return len(self.underlying)

    def __iter__(self):
        return iter(self.underlying)

    def get_random_member(self) -> Any:
        return self.underlying.get_random_member()
