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

    def test_aof_complex_types_persistence(self):
        # 1. Prepare a List object
        from core.internals.QuickList import QuickList
        ql = QuickList()
        ql.rpush(b"list_val_1")
        ql.rpush(b"list_val_2")
        list_obj = RedisObject(ql, REDIS_OBJECT_TYPES.TYPE_LIST, REDIS_OBJECT_ENCODINGS.QUICKLIST)
        Store.put("k_list", list_obj, -1, db=0)

        # 2. Prepare a Set object
        from core.internals.Set import Set
        st = Set()
        st.add("set_val_1")
        st.add("set_val_2")
        set_obj = RedisObject(st, REDIS_OBJECT_TYPES.TYPE_SET, REDIS_OBJECT_ENCODINGS.HT)
        Store.put("k_set", set_obj, -1, db=0)

        # 3. Prepare a Geo object
        from core.internals.HashMap import HashMap
        from core.internals.Geohash import GeoHashStruct
        from core.internals.Malloc_internal import MallocInternal
        import ctypes
        hm = HashMap("string", "int64")
        new_ptr = MallocInternal.zcalloc(ctypes.sizeof(GeoHashStruct))
        struct_obj = ctypes.cast(new_ptr, ctypes.POINTER(GeoHashStruct)).contents
        struct_obj.lat = 12.34
        struct_obj.lon = 56.78
        hm.set("loc1", new_ptr)
        geo_obj = RedisObject(hm, REDIS_OBJECT_TYPES.TYPE_GEO, REDIS_OBJECT_ENCODINGS.HT)
        Store.put("k_geo", geo_obj, -1, db=0)

        # Dump to AOF
        AOF.dumpAllAOF()

        # Clear stores
        Store.stores[0].clear()
        Store.expires_list[0].clear()

        # Load from AOF
        AOF.loadAllAOF()

        # Verify List restoration
        restored_list = Store.get("k_list", db=0)
        self.assertIsNotNone(restored_list)
        self.assertEqual(restored_list.getType(), REDIS_OBJECT_TYPES.TYPE_LIST)
        self.assertEqual(list(restored_list.getValue()), [b"list_val_1", b"list_val_2"])

        # Verify Set restoration
        restored_set = Store.get("k_set", db=0)
        self.assertIsNotNone(restored_set)
        self.assertEqual(restored_set.getType(), REDIS_OBJECT_TYPES.TYPE_SET)
        self.assertEqual(set(restored_set.getValue()), {"set_val_1", "set_val_2"})

        # Verify Geo restoration
        restored_geo = Store.get("k_geo", db=0)
        self.assertIsNotNone(restored_geo)
        self.assertEqual(restored_geo.getType(), REDIS_OBJECT_TYPES.TYPE_GEO)
        restored_hm = restored_geo.getValue()
        loc1_ptr = restored_hm.get("loc1")
        self.assertIsNotNone(loc1_ptr)
        geo_struct = ctypes.cast(loc1_ptr, ctypes.POINTER(GeoHashStruct)).contents
        self.assertAlmostEqual(geo_struct.lat, 12.34, places=2)
        self.assertAlmostEqual(geo_struct.lon, 56.78, places=2)

if __name__ == "__main__":
    unittest.main()
