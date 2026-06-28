import sys
import os
import unittest
import gc

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.internals.BloomFilter import BloomFilter
from core.internals.Malloc_internal import MallocInternal
from core.evaluator import Evaluator
from core.Store import Store
from core.RedisCmd import RedisCmd

class MockConnection:
    def __init__(self):
        self.sent_data = bytearray()
        self.db = 0
        
    def send(self, data):
        self.sent_data.extend(data)

class TestBloomFilter(unittest.TestCase):
    def test_bloom_filter_basic_ops(self):
        bf = BloomFilter()
        try:
            # Initially, elements should not exist
            self.assertFalse(bf.exists("hello"))
            self.assertFalse(bf.exists("world"))
            
            # Add elements and verify boolean return value
            self.assertTrue(bf.add("hello"))      # Newly added
            self.assertTrue(bf.exists("hello"))
            self.assertFalse(bf.exists("world"))
            
            self.assertFalse(bf.add("hello"))     # Already exists, should return False
            
            self.assertTrue(bf.add("world"))      # Newly added
            self.assertTrue(bf.exists("hello"))
            self.assertTrue(bf.exists("world"))
            
            # Test non-existing element
            self.assertFalse(bf.exists("other_element"))
        finally:
            bf.free()

    def test_bloom_filter_binary_safety(self):
        bf = BloomFilter()
        try:
            bin_data1 = b"data\x00with\x00nulls"
            bin_data2 = b"data\x00with\x00different_nulls"
            
            self.assertTrue(bf.add(bin_data1))
            self.assertTrue(bf.exists(bin_data1))
            self.assertFalse(bf.exists(bin_data2))
        finally:
            bf.free()

    def test_bloom_filter_pointer_wrapping(self):
        # Create a BloomFilter and get its raw pointer
        bf1 = BloomFilter()
        self.assertTrue(bf1.add("python"))
        ptr = bf1.release() # bf1 no longer owns the memory
        
        try:
            # Wrap the raw pointer in a new BloomFilter
            bf2 = BloomFilter(ptr=ptr)
            self.assertTrue(bf2.exists("python"))
            self.assertFalse(bf2.exists("java"))
            
            self.assertTrue(bf2.add("java"))
            self.assertTrue(bf2.exists("java"))
        finally:
            # Manually free the pointer since ownership was released
            from core.internals.sds import SDS
            SDS(ptr=ptr).free()

    def test_bloom_filter_memory_cleanup(self):
        gc.collect()
        mem_before = MallocInternal.zmalloc_used_memory()
        
        # Create and delete BloomFilter
        bf = BloomFilter()
        self.assertTrue(MallocInternal.zmalloc_used_memory() > mem_before)
        
        del bf
        gc.collect()
        
        mem_after = MallocInternal.zmalloc_used_memory()
        self.assertEqual(mem_after, mem_before)

    def test_evaluator_bloom_filter_commands(self):
        conn = MockConnection()
        db = 0
        
        # Clean up key if it exists
        Store.delete("my_bloom_filter", db)
        
        # 1. BF.EXISTS on non-existent key should return :0\r\n
        cmd1 = RedisCmd("BFEXISTS", ["my_bloom_filter", "item1"])
        Evaluator.evalAndRespond([cmd1], conn)
        self.assertEqual(conn.sent_data, b":0\r\n")
        conn.sent_data.clear()
        
        # 2. BF.ADD item1 -> should return :1\r\n (newly added)
        cmd2 = RedisCmd("BFADD", ["my_bloom_filter", "item1"])
        Evaluator.evalAndRespond([cmd2], conn)
        self.assertEqual(conn.sent_data, b":1\r\n")
        conn.sent_data.clear()
        
        # 3. BF.EXISTS item1 -> should return :1\r\n
        cmd3 = RedisCmd("BFEXISTS", ["my_bloom_filter", "item1"])
        Evaluator.evalAndRespond([cmd3], conn)
        self.assertEqual(conn.sent_data, b":1\r\n")
        conn.sent_data.clear()
        
        # 4. BF.ADD item1 again -> should return :0\r\n (already exists)
        cmd4 = RedisCmd("BFADD", ["my_bloom_filter", "item1"])
        Evaluator.evalAndRespond([cmd4], conn)
        self.assertEqual(conn.sent_data, b":0\r\n")
        conn.sent_data.clear()
        
        # 5. BF.EXISTS item2 -> should return :0\r\n
        cmd5 = RedisCmd("BFEXISTS", ["my_bloom_filter", "item2"])
        Evaluator.evalAndRespond([cmd5], conn)
        self.assertEqual(conn.sent_data, b":0\r\n")
        conn.sent_data.clear()
        
        # Cleanup
        Store.delete("my_bloom_filter", db)

if __name__ == "__main__":
    unittest.main()
