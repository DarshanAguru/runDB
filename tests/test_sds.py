import sys
import os
import unittest
import gc

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.internals.sds import SDS, sdsHelpers
from core.internals.Malloc_internal import MallocInternal
from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS

class TestSDS(unittest.TestCase):
    def test_sds_basic_ops(self):
        # Create new SDS
        sds_ptr = sdsHelpers.sdsnew("hello")
        try:
            self.assertEqual(sdsHelpers.sdslen(sds_ptr), 5)
            self.assertEqual(sdsHelpers.sdsavail(sds_ptr), 0)
            self.assertEqual(sdsHelpers.sdsrepr(sds_ptr), b"hello")
            
            # Copy new content (smaller)
            sds_ptr = sdsHelpers.sdscpy(sds_ptr, b"hi")
            self.assertEqual(sdsHelpers.sdslen(sds_ptr), 2)
            self.assertEqual(sdsHelpers.sdsrepr(sds_ptr), b"hi")
            
            # Copy new content (larger, triggers realloc)
            sds_ptr = sdsHelpers.sdscpy(sds_ptr, b"hello world")
            self.assertEqual(sdsHelpers.sdslen(sds_ptr), 11)
            self.assertEqual(sdsHelpers.sdsrepr(sds_ptr), b"hello world")
            
            # Append content
            sds_ptr = sdsHelpers.sdscat(sds_ptr, "!!!")
            self.assertEqual(sdsHelpers.sdslen(sds_ptr), 14)
            self.assertEqual(sdsHelpers.sdsrepr(sds_ptr), b"hello world!!!")
        finally:
            sdsHelpers.sdsfree(sds_ptr)

    def test_sds_binary_safety(self):
        # SDS should support embedded null bytes
        binary_data = b"hello\x00world\x00"
        sds_ptr = sdsHelpers.sdsnewlen(binary_data, len(binary_data))
        try:
            self.assertEqual(sdsHelpers.sdslen(sds_ptr), 12)
            self.assertEqual(sdsHelpers.sdsrepr(sds_ptr), binary_data)
        finally:
            sdsHelpers.sdsfree(sds_ptr)

    def test_sds_wrapper_class(self):
        sds_obj = SDS("test_string")
        self.assertEqual(len(sds_obj), 11)
        self.assertEqual(str(sds_obj), "test_string")
        self.assertEqual(bytes(sds_obj), b"test_string")
        
        # Append
        sds_obj.append(" appended")
        self.assertEqual(len(sds_obj), 20)
        self.assertEqual(str(sds_obj), "test_string appended")
        
        # Copy
        sds_obj.copy(b"new")
        self.assertEqual(len(sds_obj), 3)
        self.assertEqual(str(sds_obj), "new")

    def test_sds_memory_cleanup_wrapper(self):
        # Record memory before
        gc.collect()
        mem_before = MallocInternal.zmalloc_used_memory()
        
        # Create and delete SDS wrapper
        sds_obj = SDS("a" * 1000)
        self.assertTrue(MallocInternal.zmalloc_used_memory() > mem_before)
        
        # Free memory
        del sds_obj
        gc.collect()
        
        mem_after = MallocInternal.zmalloc_used_memory()
        self.assertEqual(mem_after, mem_before)

    def test_sds_memory_cleanup_redis_object(self):
        # Record memory before
        gc.collect()
        mem_before = MallocInternal.zmalloc_used_memory()
        
        # Create RedisObject with RAW string
        obj = RedisObject("some large raw string data " * 100, REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.RAW)
        self.assertTrue(MallocInternal.zmalloc_used_memory() > mem_before)
        
        # Access and verify value
        self.assertTrue(obj.getValue().startswith("some large"))
        
        # Delete and verify memory is fully reclaimed
        del obj
        gc.collect()
        
        mem_after = MallocInternal.zmalloc_used_memory()
        self.assertEqual(mem_after, mem_before)

    def test_sds_header_types(self):
        # 1. Short string (10 bytes) -> SDS_TYPE_5 (0)
        s1 = sdsHelpers.sdsnewlen(b"a" * 10, 10)
        try:
            self.assertEqual(sdsHelpers.sds_get_type(s1), 0)
            self.assertEqual(sdsHelpers.sdslen(s1), 10)
            
            # Append to grow beyond 32 bytes -> Upgrades to SDS_TYPE_8 (1)
            s1 = sdsHelpers.sdscat(s1, b"b" * 30)
            self.assertEqual(sdsHelpers.sds_get_type(s1), 1)
            self.assertEqual(sdsHelpers.sdslen(s1), 40)
        finally:
            sdsHelpers.sdsfree(s1)
            
        # 2. Medium string (300 bytes) -> SDS_TYPE_16 (2)
        s2 = sdsHelpers.sdsnewlen(b"c" * 300, 300)
        try:
            self.assertEqual(sdsHelpers.sds_get_type(s2), 2)
            self.assertEqual(sdsHelpers.sdslen(s2), 300)
        finally:
            sdsHelpers.sdsfree(s2)

if __name__ == "__main__":
    unittest.main()
