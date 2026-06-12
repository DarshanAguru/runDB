import sys
import os
import unittest
import time
from config import Config

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.Store import Store
from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS
from core.aof import AOF

class TestAOF(unittest.TestCase):
    def setUp(self):
        self.original_aof_file = Config.AOF_FILE
        Config.AOF_FILE = "test_aof_temp.aof"
        if os.path.exists(Config.AOF_FILE):
            os.remove(Config.AOF_FILE)
            
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()

    def tearDown(self):
        Config.AOF_FILE = self.original_aof_file
        if os.path.exists("test_aof_temp.aof"):
            os.remove("test_aof_temp.aof")

    def test_aof_expiration_persistence(self):
        obj = RedisObject("exp_val", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        # Store with 10 seconds expiration
        Store.put("k_exp", obj, 10, db=0)
        
        # Dump to AOF
        AOF.dumpAllAOF()
        
        # Clear stores
        Store.stores[0].clear()
        Store.expires_list[0].clear()
        self.assertIsNone(Store.get("k_exp", db=0))
        
        # Load from AOF
        AOF.loadAllAOF()
        
        # Verify restored key and remaining TTL
        restored_obj = Store.get("k_exp", db=0)
        self.assertIsNotNone(restored_obj)
        self.assertEqual(restored_obj.getValue(), "exp_val")
        
        expiry = Store.getExpiry(restored_obj, db=0)
        self.assertNotEqual(expiry, -1)
        remaining_ttl = expiry - int(time.time())
        self.assertTrue(0 < remaining_ttl <= 10)

if __name__ == "__main__":
    unittest.main()
