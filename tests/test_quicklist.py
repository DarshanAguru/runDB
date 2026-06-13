import unittest
import sys
import os

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.internals.QuickList import ZipList, QuickList, ZipListEntry
from core.internals.Malloc_internal import MemTracker


class TestZipList(unittest.TestCase):

    def test_ziplist_empty(self):
        zl = ZipList()
        self.assertEqual(len(zl), 0)
        self.assertEqual(zl.size, 13)
        self.assertEqual(list(zl), [])

    def test_ziplist_push_pop(self):
        zl = ZipList()
        zl.rpush(b"hello")
        zl.rpush(b"world")
        self.assertEqual(len(zl), 2)
        self.assertEqual(list(zl), [b"hello", b"world"])

        zl.lpush(b"first")
        self.assertEqual(list(zl), [b"first", b"hello", b"world"])

        self.assertEqual(zl.lpop(), b"first")
        self.assertEqual(zl.rpop(), b"world")
        self.assertEqual(list(zl), [b"hello"])
        self.assertEqual(zl.lpop(), b"hello")
        self.assertEqual(len(zl), 0)

    def test_ziplist_indexing(self):
        zl = ZipList()
        zl.rpush(b"a")
        zl.rpush(b"b")
        zl.rpush(b"c")

        self.assertEqual(zl[0], b"a")
        self.assertEqual(zl[1], b"b")
        self.assertEqual(zl[2], b"c")
        self.assertEqual(zl[-1], b"c")
        self.assertEqual(zl[-2], b"b")
        self.assertEqual(zl[-3], b"a")

        with self.assertRaises(IndexError):
            _ = zl[3]
        with self.assertRaises(IndexError):
            _ = zl[-4]

    def test_ziplist_insert_and_delete(self):
        zl = ZipList()
        zl.rpush(b"a")
        zl.rpush(b"c")

        # Insert "b" between "a" and "c"
        offset = zl.get_offset_at_index(1)
        zl.insert_at_offset(offset, b"b")
        self.assertEqual(list(zl), [b"a", b"b", b"c"])

        # Insert at head
        offset = zl.get_offset_at_index(0)
        zl.insert_at_offset(offset, b"start")
        self.assertEqual(list(zl), [b"start", b"a", b"b", b"c"])

        # Insert at tail
        offset = zl.get_offset_at_index(len(zl) - 1)
        # To insert after the last element, get offset after last element
        zlbytes = zl.size
        zl.insert_at_offset(zlbytes - 1, b"end")
        self.assertEqual(list(zl), [b"start", b"a", b"b", b"c", b"end"])

        # Delete from middle
        offset = zl.get_offset_at_index(2)  # "b"
        zl.delete_at_offset(offset)
        self.assertEqual(list(zl), [b"start", b"a", b"c", b"end"])

        # Delete head
        offset = zl.get_offset_at_index(0)
        zl.delete_at_offset(offset)
        self.assertEqual(list(zl), [b"a", b"c", b"end"])

        # Delete tail
        offset = zl.get_offset_at_index(len(zl) - 1)
        zl.delete_at_offset(offset)
        self.assertEqual(list(zl), [b"a", b"c"])

    def test_redis_encoding_details(self):
        # Test small integer (0 to 12) direct encoding
        zl = ZipList()
        zl.rpush(b"5")
        # For 5, direct encoding is used:
        # prev_len = 0 (1 byte)
        # encoding = 0xF0 | (5+1) = 0xF6 (1 byte)
        # data = 0 bytes
        # Total entry = 2 bytes
        # Total size = 12 (header) + 2 (entry) + 1 (end marker) = 15 bytes
        self.assertEqual(zl.size, 15)
        self.assertEqual(zl[0], b"5")

        # Test 8-bit integer encoding (-128 to 127)
        zl2 = ZipList()
        zl2.rpush(b"100")
        # For 100, encoding is 0xFE (1 byte), data is 1 byte
        # prev_len = 0 (1 byte)
        # Total entry = 3 bytes
        # Total size = 12 + 3 + 1 = 16 bytes
        self.assertEqual(zl2.size, 16)
        self.assertEqual(zl2[0], b"100")

        # Test string encoding
        zl3 = ZipList()
        zl3.rpush(b"hello")
        # For "hello", len is 5. Encoding is 5 (1 byte), data is 5 bytes
        # prev_len = 0 (1 byte)
        # Total entry = 7 bytes
        # Total size = 12 + 7 + 1 = 20 bytes
        self.assertEqual(zl3.size, 20)
        self.assertEqual(zl3[0], b"hello")


class TestQuickList(unittest.TestCase):

    def test_quicklist_empty(self):
        ql = QuickList()
        self.assertEqual(len(ql), 0)
        self.assertEqual(ql.node_count, 0)
        self.assertEqual(list(ql), [])

    def test_quicklist_push_pop(self):
        ql = QuickList(max_entries=2)
        ql.rpush(b"1")
        ql.rpush(b"2")
        ql.rpush(b"3")

        # Since max_entries=2, "1" and "2" should be in node 1, "3" in node 2.
        self.assertEqual(ql.node_count, 2)
        self.assertEqual(list(ql), [b"1", b"2", b"3"])

        ql.lpush(b"0")
        # "0" should be pushed to the head node. But head node already has 2 entries.
        # So a new head node should be created containing "0".
        self.assertEqual(ql.node_count, 3)
        self.assertEqual(list(ql), [b"0", b"1", b"2", b"3"])

        self.assertEqual(ql.lpop(), b"0")
        self.assertEqual(ql.node_count, 2)  # The node for "0" should be removed as it becomes empty
        self.assertEqual(list(ql), [b"1", b"2", b"3"])

        self.assertEqual(ql.rpop(), b"3")
        self.assertEqual(ql.node_count, 1)  # The second node should be removed
        self.assertEqual(list(ql), [b"1", b"2"])

    def test_quicklist_indexing(self):
        ql = QuickList(max_entries=2)
        for val in [b"a", b"b", b"c", b"d", b"e"]:
            ql.rpush(val)

        self.assertEqual(ql.node_count, 3)
        self.assertEqual(len(ql), 5)
        self.assertEqual(ql[0], b"a")
        self.assertEqual(ql[2], b"c")
        self.assertEqual(ql[4], b"e")
        self.assertEqual(ql[-1], b"e")
        self.assertEqual(ql[-3], b"c")

        with self.assertRaises(IndexError):
            _ = ql[5]

    def test_quicklist_insert_split(self):
        # We set max_entries=2
        ql = QuickList(max_entries=2)
        ql.rpush(b"a")
        ql.rpush(b"c")  # Node 1 is full (entries: a, c)

        # Insert "b" at index 1. Since Node 1 is full, it must split!
        ql.insert(1, b"b")
        # Split behavior:
        # left = [a], left.append(b) -> [a, b] (2 entries)
        # right = [c] -> [c] (1 entry)
        # Total nodes should be 2.
        self.assertEqual(ql.node_count, 2)
        self.assertEqual(list(ql), [b"a", b"b", b"c"])

    def test_quicklist_delete(self):
        ql = QuickList(max_entries=2)
        for val in [b"a", b"b", b"c"]:
            ql.rpush(val)

        self.assertEqual(ql.node_count, 2)
        # Delete middle element "b" at index 1
        ql.delete(1)
        self.assertEqual(list(ql), [b"a", b"c"])
        self.assertEqual(ql.node_count, 2)  # Neither node is empty (Node 1 has "a", Node 2 has "c")

        # Delete "c" at index 1
        ql.delete(1)
        self.assertEqual(list(ql), [b"a"])
        self.assertEqual(ql.node_count, 1)  # Node 2 becomes empty and is removed.

    def test_quicklist_delete_empty_node_cleanup(self):
        # With max_entries=1, each element gets its own node
        ql = QuickList(max_entries=1)
        for val in [b"a", b"b", b"c"]:
            ql.rpush(val)

        self.assertEqual(ql.node_count, 3)
        # Delete "b" at index 1 (the middle node)
        ql.delete(1)
        self.assertEqual(list(ql), [b"a", b"c"])
        self.assertEqual(ql.node_count, 2)  # Node 2 became empty and was removed

    def test_memory_cleanup(self):
        initial_stats = MemTracker.stats()
        
        ql = QuickList(max_entries=3)
        for i in range(10):
            ql.rpush(f"val-{i}".encode())
            
        stats_allocated = MemTracker.stats()
        self.assertGreater(stats_allocated["bytes"], initial_stats["bytes"])
        self.assertGreater(stats_allocated["blocks"], initial_stats["blocks"])
        
        # Free the list
        ql.free()
        
        final_stats = MemTracker.stats()
        self.assertEqual(final_stats["bytes"], initial_stats["bytes"])
        self.assertEqual(final_stats["blocks"], initial_stats["blocks"])


if __name__ == "__main__":
    unittest.main()
