import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.evaluator import Evaluator
from core.RedisCmd import RedisCmd
from core.Store import Store
from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS
from core.internals.Malloc_internal import MemTracker


class MockClient:
    def __init__(self):
        self.cqueue = []
        self.isTrans = False
        self.db = 0
        self.sent_data = bytearray()

    def send(self, data: bytes) -> int:
        self.sent_data.extend(data)
        return len(data)

    def TransBegin(self):
        self.isTrans = True

    def TransExec(self) -> bytes:
        from core.evaluator import Evaluator
        class ResponseAccumulator:
            def __init__(self):
                self.buffer = bytearray()
            def send(self, data: bytes) -> int:
                self.buffer.extend(data)
                return len(data)
        
        accumulator = ResponseAccumulator()
        for cmd in self.cqueue:
            Evaluator.evalAndRespond([cmd], accumulator)
        
        response = f"*{len(self.cqueue)}\r\n".encode("utf-8") + accumulator.buffer
        self.isTrans = False
        self.cqueue = []
        return response

    def TransDiscard(self):
        self.isTrans = False
        self.cqueue = []


class TestListCommands(unittest.TestCase):
    def setUp(self):
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()
        self.client = MockClient()

    def test_lpush_rpush_llen(self):
        # LPUSH on non-existent creates a list
        cmd = RedisCmd("LPUSH", ["mylist", "a", "b"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b":2\r\n")  # Returns length

        # RPUSH appends to same list
        self.client.sent_data.clear()
        cmd = RedisCmd("RPUSH", ["mylist", "c", "d"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b":4\r\n")

        # LLEN checks length
        self.client.sent_data.clear()
        cmd = RedisCmd("LLEN", ["mylist"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b":4\r\n")

        # LLEN on non-existent returns 0
        self.client.sent_data.clear()
        cmd = RedisCmd("LLEN", ["nonexistent"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b":0\r\n")

    def test_lpop_rpop_cleanup(self):
        cmd = RedisCmd("RPUSH", ["mylist", "first", "second", "third"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.client.sent_data.clear()

        # LPOP retrieves first
        cmd = RedisCmd("LPOP", ["mylist"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$5\r\nfirst\r\n")

        # RPOP retrieves last
        self.client.sent_data.clear()
        cmd = RedisCmd("RPOP", ["mylist"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$5\r\nthird\r\n")

        # Pop remaining
        self.client.sent_data.clear()
        cmd = RedisCmd("LPOP", ["mylist"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$6\r\nsecond\r\n")

        # List is now empty. Key should be removed.
        self.assertIsNone(Store.get("mylist"))

        # Pop on empty/non-existent returns nil
        self.client.sent_data.clear()
        cmd = RedisCmd("LPOP", ["mylist"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$-1\r\n")

    def test_lindex_lrange(self):
        cmd = RedisCmd("RPUSH", ["mylist", "a", "b", "c", "d"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.client.sent_data.clear()

        # LINDEX positive and negative
        cmd = RedisCmd("LINDEX", ["mylist", "1"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$1\r\nb\r\n")

        self.client.sent_data.clear()
        cmd = RedisCmd("LINDEX", ["mylist", "-1"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$1\r\nd\r\n")

        # LINDEX out of range returns nil
        self.client.sent_data.clear()
        cmd = RedisCmd("LINDEX", ["mylist", "10"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"$-1\r\n")

        # LRANGE positive slice
        self.client.sent_data.clear()
        cmd = RedisCmd("LRANGE", ["mylist", "1", "2"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n")

        # LRANGE inclusive negative slice
        self.client.sent_data.clear()
        cmd = RedisCmd("LRANGE", ["mylist", "0", "-1"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")

    def test_wrongtype_errors(self):
        # Create a string key
        from core.RedisObject import RedisObject
        obj = RedisObject("myval", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        Store.put("mystring", obj, -1)

        # Try list operations on a string key
        cmd = RedisCmd("LPUSH", ["mystring", "item"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertIn(b"WRONGTYPE", self.client.sent_data)

        self.client.sent_data.clear()
        cmd = RedisCmd("LPOP", ["mystring"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertIn(b"WRONGTYPE", self.client.sent_data)

    def test_debug_object(self):
        # Create a string key
        from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS
        obj = RedisObject("somevalue", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        Store.put("mykey", obj, -1)

        # Call DEBUG OBJECT
        cmd = RedisCmd("DEBUG", ["OBJECT", "mykey"])
        Evaluator.evalAndRespond([cmd], self.client)
        
        response = self.client.sent_data.decode("utf-8")
        self.assertTrue(response.startswith("+Value at:"))
        self.assertIn("refcount:1", response)
        self.assertIn("encoding:embstr", response)
        self.assertIn("serializedlength:10", response)
        self.assertIn("lru:", response)
        self.assertIn("lru_seconds_idle:", response)

    def test_memory_cleanup_on_delete(self):
        initial_stats = MemTracker.stats()

        # Push items to list
        cmd = RedisCmd("RPUSH", ["mylist", "x", "y", "z"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.client.sent_data.clear()

        allocated_stats = MemTracker.stats()
        self.assertGreater(allocated_stats["bytes"], initial_stats["bytes"])

        # Delete the list
        cmd = RedisCmd("DEL", ["mylist"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b":1\r\n")

        # Run garbage collection explicitly to trigger weakref callbacks
        import gc
        gc.collect()

        final_stats = MemTracker.stats()
        self.assertEqual(final_stats["bytes"], initial_stats["bytes"])


if __name__ == "__main__":
    unittest.main()
