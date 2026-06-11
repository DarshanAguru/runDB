import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.evaluator import Evaluator
from core.RedisCmd import RedisCmd
from core.Store import Store

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

class TestEvaluator(unittest.TestCase):
    def setUp(self):
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()
        self.client = MockClient()

    def test_ping(self):
        cmd = RedisCmd("PING", [])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b"+PONG\r\n")

        self.client.sent_data.clear()
        cmd_args = RedisCmd("PING", ["hello"])
        Evaluator.evalAndRespond([cmd_args], self.client)
        self.assertEqual(self.client.sent_data, b"$5\r\nhello\r\n")

    def test_set_and_get(self):
        cmd_set = RedisCmd("SET", ["mykey", "myval"])
        Evaluator.evalAndRespond([cmd_set], self.client)
        self.assertEqual(self.client.sent_data, b"+OK\r\n")

        self.client.sent_data.clear()
        cmd_get = RedisCmd("GET", ["mykey"])
        Evaluator.evalAndRespond([cmd_get], self.client)
        self.assertEqual(self.client.sent_data, b"$5\r\nmyval\r\n")

    def test_incr(self):
        # INCR on non-existing key sets it to 1
        cmd_incr = RedisCmd("INCR", ["num"])
        Evaluator.evalAndRespond([cmd_incr], self.client)
        self.assertEqual(self.client.sent_data, b":1\r\n")

        # INCR again increments it to 2
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([cmd_incr], self.client)
        self.assertEqual(self.client.sent_data, b":2\r\n")

    def test_del(self):
        # Set a key
        Store.put("k", None, -1) # Wait, RedisObject needs to be created
        from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS
        obj = RedisObject("v", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        Store.put("k", obj, -1)

        cmd_del = RedisCmd("DEL", ["k", "non_existing"])
        Evaluator.evalAndRespond([cmd_del], self.client)
        self.assertEqual(self.client.sent_data, b":1\r\n") # 1 key deleted

    def test_transactions_multi_exec(self):
        # MULTI command
        cmd_multi = RedisCmd("MULTI", [])
        Evaluator.evalAndRespond([cmd_multi], self.client)
        self.assertEqual(self.client.sent_data, b"+OK\r\n")
        self.assertTrue(self.client.isTrans)

        # Queue command
        self.client.sent_data.clear()
        cmd_set = RedisCmd("SET", ["k", "v"])
        Evaluator.evalAndRespond([cmd_set], self.client)
        self.assertEqual(self.client.sent_data, b"+QUEUED\r\n")
        self.assertEqual(len(self.client.cqueue), 1)

        # EXEC command
        self.client.sent_data.clear()
        cmd_exec = RedisCmd("EXEC", [])
        Evaluator.evalAndRespond([cmd_exec], self.client)
        # EXEC response: array of replies. Here it executes SET -> +OK\r\n
        self.assertEqual(self.client.sent_data, b"*1\r\n+OK\r\n")
        self.assertFalse(self.client.isTrans)

if __name__ == "__main__":
    unittest.main()
