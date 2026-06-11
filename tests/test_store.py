import sys
import os
import unittest
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.Store import Store
from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS

class TestStore(unittest.TestCase):
    def setUp(self):
        # Clean up database stores before each test
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()

    def test_put_and_get(self):
        obj = RedisObject("val1", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        Store.put("k1", obj, -1, db=0)
        
        retrieved = Store.get("k1", db=0)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.getValue(), "val1")

    def test_db_isolation(self):
        obj1 = RedisObject("db0_val", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        obj2 = RedisObject("db1_val", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        
        Store.put("key", obj1, -1, db=0)
        Store.put("key", obj2, -1, db=1)
        
        self.assertEqual(Store.get("key", db=0).getValue(), "db0_val")
        self.assertEqual(Store.get("key", db=1).getValue(), "db1_val")

    def test_delete(self):
        obj = RedisObject("val", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        Store.put("k", obj, -1, db=0)
        self.assertTrue(Store.delete("k", db=0))
        self.assertIsNone(Store.get("k", db=0))
        self.assertFalse(Store.delete("k", db=0)) # already deleted

    def test_expiration(self):
        obj = RedisObject("exp_val", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        # Expire in 1 second
        Store.put("k_exp", obj, 1, db=0)
        
        self.assertFalse(Store.hasExpired(obj, db=0))
        self.assertEqual(Store.get("k_exp", db=0).getValue(), "exp_val")
        
        # Wait for expiration
        time.sleep(1.1)
        self.assertTrue(Store.hasExpired(obj, db=0))
        self.assertIsNone(Store.get("k_exp", db=0)) # should be lazy-deleted

if __name__ == "__main__":
    unittest.main()
