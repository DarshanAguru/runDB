import unittest
import sys
import os
import ctypes

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.internals.Intset import Intset
from core.internals.HashTable import HashTable
from core.internals.Set import Set
from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS
from core.evaluator import Evaluator
from core.RedisCmd import RedisCmd
from core.Store import Store

# A mock connection client to evaluate commands
class MockClient:
    def __init__(self):
        self.sent_data = bytearray()
        self.db = 0
    def send(self, data: bytes):
        self.sent_data.extend(data)
        return len(data)

class TestSet(unittest.TestCase):
    def setUp(self):
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()

    def test_intset_basics(self):
        iset = Intset()
        self.assertEqual(len(iset), 0)
        
        # Test sorted insertion
        self.assertTrue(iset.add(10))
        self.assertTrue(iset.add(5))
        self.assertTrue(iset.add(20))
        # Prevent duplicates
        self.assertFalse(iset.add(10))
        
        self.assertEqual(list(iset), [5, 10, 20])
        
        # Test upgrade
        self.assertTrue(iset.add(40000)) # requires 32-bit
        self.assertTrue(iset.add(5000000000)) # requires 64-bit
        
        self.assertEqual(list(iset), [5, 10, 20, 40000, 5000000000])
        
        # Test remove
        self.assertTrue(iset.remove(10))
        self.assertFalse(iset.remove(10))
        self.assertEqual(list(iset), [5, 20, 40000, 5000000000])

    def test_set_auto_upgrade(self):
        # Set threshold to 3 for testing
        import core.internals.Set as set_mod
        original_threshold = set_mod.SET_MAX_INTSET_ENTRIES
        set_mod.SET_MAX_INTSET_ENTRIES = 3
        
        try:
            s = Set()
            self.assertEqual(s.encoding, 11) # INTSET
            
            s.add(1)
            s.add(2)
            s.add(3)
            self.assertEqual(s.encoding, 11)
            
            # Exceeding threshold triggers upgrade
            s.add(4)
            self.assertEqual(s.encoding, 12) # HT
            self.assertEqual(len(s), 4)
            self.assertTrue("1" in s)
            self.assertTrue("4" in s)
            
            # Non-integer triggers upgrade immediately
            s2 = Set()
            s2.add(10)
            self.assertEqual(s2.encoding, 11)
            s2.add("hello")
            self.assertEqual(s2.encoding, 12)
            self.assertEqual(len(s2), 2)
            self.assertTrue("10" in s2)
            self.assertTrue("hello" in s2)
        finally:
            set_mod.SET_MAX_INTSET_ENTRIES = original_threshold

    def test_evaluator_set_commands(self):
        client = MockClient()
        
        # SADD to new set
        cmd = RedisCmd("SADD", ["myset", "10", "20"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":2\r\n")
        client.sent_data.clear()
        
        # SISMEMBER
        cmd = RedisCmd("SISMEMBER", ["myset", "10"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":1\r\n")
        client.sent_data.clear()
        
        cmd = RedisCmd("SISMEMBER", ["myset", "30"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":0\r\n")
        client.sent_data.clear()
        
        # SCARD
        cmd = RedisCmd("SCARD", ["myset"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":2\r\n")
        client.sent_data.clear()
        
        # SMEMBERS
        cmd = RedisCmd("SMEMBERS", ["myset"])
        Evaluator.evalAndRespond([cmd], client)
        # RESP array of members (order might be sorted since it is currently INTSET)
        self.assertEqual(client.sent_data, b"*2\r\n$2\r\n10\r\n$2\r\n20\r\n")
        client.sent_data.clear()
        
        # SRANDMEMBER
        cmd = RedisCmd("SRANDMEMBER", ["myset"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertTrue(client.sent_data in (b"$2\r\n10\r\n", b"$2\r\n20\r\n"))
        client.sent_data.clear()

        # SRANDMEMBER with count
        cmd = RedisCmd("SRANDMEMBER", ["myset", "2"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertTrue(client.sent_data in (b"*2\r\n$2\r\n10\r\n$2\r\n20\r\n", b"*2\r\n$2\r\n20\r\n$2\r\n10\r\n"))
        client.sent_data.clear()
        
        # SREM
        cmd = RedisCmd("SREM", ["myset", "10"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":1\r\n")
        client.sent_data.clear()
        
        # SCARD should now be 1
        cmd = RedisCmd("SCARD", ["myset"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":1\r\n")
        client.sent_data.clear()

        # Remove last item to verify empty set deletion
        cmd = RedisCmd("SREM", ["myset", "20"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertEqual(client.sent_data, b":1\r\n")
        client.sent_data.clear()

        # Key should be deleted
        self.assertIsNone(Store.get("myset", 0))

    def test_wrongtype_errors(self):
        client = MockClient()
        
        # SET key to string
        Store.put("mystring", RedisObject("value", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.RAW), -1, 0)
        
        # SADD on string should return WRONGTYPE error
        cmd = RedisCmd("SADD", ["mystring", "10"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertTrue(b"WRONGTYPE" in client.sent_data)
        client.sent_data.clear()

    def test_debug_object_set(self):
        client = MockClient()
        
        # SADD elements
        cmd = RedisCmd("SADD", ["myset", "10"])
        Evaluator.evalAndRespond([cmd], client)
        client.sent_data.clear()
        
        # DEBUG OBJECT
        cmd = RedisCmd("DEBUG", ["OBJECT", "myset"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertTrue(b"encoding:intset" in client.sent_data)
        client.sent_data.clear()

        # Trigger upgrade to HashTable by adding non-int
        cmd = RedisCmd("SADD", ["myset", "hello"])
        Evaluator.evalAndRespond([cmd], client)
        client.sent_data.clear()

        # DEBUG OBJECT again
        cmd = RedisCmd("DEBUG", ["OBJECT", "myset"])
        Evaluator.evalAndRespond([cmd], client)
        self.assertTrue(b"encoding:hashtable" in client.sent_data)
        client.sent_data.clear()
