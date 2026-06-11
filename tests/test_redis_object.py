import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS

class TestRedisObject(unittest.TestCase):
    def test_slots_defined(self):
        # Verify slots are defined to protect against dict usage
        obj = RedisObject("test", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        self.assertFalse(hasattr(obj, "__dict__"))

    def test_redis_object_creation_string(self):
        obj = RedisObject("my_string", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        self.assertEqual(obj.getType(), REDIS_OBJECT_TYPES.TYPE_STRING)
        self.assertEqual(obj.getEncoding(), REDIS_OBJECT_ENCODINGS.EMBSTR)
        self.assertEqual(obj.getValue(), "my_string")

    def test_redis_object_creation_int(self):
        obj = RedisObject("12345", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.INT)
        self.assertEqual(obj.getType(), REDIS_OBJECT_TYPES.TYPE_STRING)
        self.assertEqual(obj.getEncoding(), REDIS_OBJECT_ENCODINGS.INT)
        self.assertEqual(obj.getValue(), 12345)

    def test_update_value(self):
        obj = RedisObject("old_value", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        self.assertEqual(obj.getValue(), "old_value")
        obj.val = "new_value"
        self.assertEqual(obj.getValue(), "new_value")

    def test_update_lat(self):
        obj = RedisObject("lat_test", REDIS_OBJECT_TYPES.TYPE_STRING, REDIS_OBJECT_ENCODINGS.EMBSTR)
        lat_before = obj.getLAT()
        obj.updateLAT()
        lat_after = obj.getLAT()
        self.assertTrue(lat_after >= lat_before)

if __name__ == "__main__":
    unittest.main()
