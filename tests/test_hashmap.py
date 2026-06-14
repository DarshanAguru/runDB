import unittest
import sys
import os
import ctypes

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.internals.HashMap import HashMap
from core.internals.Malloc_internal import MallocInternal

class TestHashMap(unittest.TestCase):
    def test_basic_ops(self):
        h = HashMap("string", "string")
        self.assertEqual(len(h), 0)
        
        h.set("hello", "world")
        self.assertEqual(len(h), 1)
        self.assertTrue("hello" in h)
        self.assertEqual(h.get("hello"), "world")
        self.assertEqual(h["hello"], "world")
        
        h["foo"] = "bar"
        self.assertEqual(len(h), 2)
        self.assertEqual(h.get("foo"), "bar")
        
        self.assertTrue(h.delete("hello"))
        self.assertEqual(len(h), 1)
        self.assertFalse("hello" in h)
        self.assertEqual(h.get("hello"), None)
        
        h.clear()
        self.assertEqual(len(h), 0)
        self.assertFalse("foo" in h)

    def test_types(self):
        # int to string
        h1 = HashMap("int", "string")
        h1.set(123, "one two three")
        self.assertEqual(h1.get(123), "one two three")
        with self.assertRaises(KeyError):
            h1.set("not-an-int", "val")
            
        # string to int
        h2 = HashMap("string", "int")
        h2.set("count", 42)
        self.assertEqual(h2.get("count"), 42)
        with self.assertRaises(ValueError):
            h2.set("count", "not-an-int")

    def test_resize(self):
        h = HashMap("string", "string")
        # Set 50 elements to trigger multiple resizes (initial cap is 16)
        for i in range(50):
            h[f"key_{i}"] = f"val_{i}"
            
        self.assertEqual(len(h), 50)
        for i in range(50):
            self.assertEqual(h[f"key_{i}"], f"val_{i}")

    def test_iterators(self):
        h = HashMap("string", "string")
        h["a"] = "1"
        h["b"] = "2"
        h["c"] = "3"
        
        self.assertEqual(sorted(list(h.keys())), ["a", "b", "c"])
        self.assertEqual(sorted(list(h.values())), ["1", "2", "3"])
        self.assertEqual(sorted(h.items()), [("a", "1"), ("b", "2"), ("c", "3")])

    def test_random(self):
        h = HashMap("string", "string")
        h["a"] = "1"
        h["b"] = "2"
        h["c"] = "3"
        
        rand_key = h.get_random_key()
        self.assertIn(rand_key, ["a", "b", "c"])
        
        rand_k, rand_v = h.get_random_item()
        self.assertIn(rand_k, ["a", "b", "c"])
        self.assertEqual(h[rand_k], rand_v)

    def test_memory_cleanup(self):
        # We perform some allocations and then check that freeing them doesn't crash and cleans up properly.
        h = HashMap("string", "string")
        h["a"] = "x"
        h["b"] = "y"
        
        # Explicit release and free
        ptr = h.release()
        self.assertFalse(h.has_ownership)
        
        # Reconstruct HashMap to take ownership and free it
        h2 = HashMap("string", "string", ptr=ptr)
        h2.has_ownership = True
        h2.free()
