import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.evaluator import Evaluator
from core.RedisCmd import RedisCmd
from core.Store import Store
from core.RedisObject import RedisObject, REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS
from tests.test_evaluator import MockClient

class TestHLL(unittest.TestCase):
    def setUp(self):
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()
        self.client = MockClient()

    def test_pfadd_and_pfcount_basic(self):
        # PFADD on non-existing key
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["hll1", "foo", "bar", "zap"])], self.client)
        self.assertEqual(self.client.sent_data, b":1\r\n")
        
        # PFADD again with same elements should return 0
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["hll1", "foo", "bar"])], self.client)
        self.assertEqual(self.client.sent_data, b":0\r\n")
        
        # PFCOUNT on the key
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["hll1"])], self.client)
        self.assertEqual(self.client.sent_data, b":3\r\n")
        
        # PFCOUNT on non-existing key should return 0
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["non_existing"])], self.client)
        self.assertEqual(self.client.sent_data, b":0\r\n")

    def test_hll_accuracy(self):
        # Add 5,000 unique elements
        elements = [f"elem_{i}" for i in range(5000)]
        # We can add them in chunks
        chunk_size = 1000
        for i in range(0, len(elements), chunk_size):
            chunk = elements[i:i+chunk_size]
            Evaluator.evalAndRespond([RedisCmd("PFADD", ["myhll"] + chunk)], self.client)
            
        # Get count
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["myhll"])], self.client)
        
        resp_data = self.client.sent_data.decode()
        self.assertTrue(resp_data.startswith(":"))
        count = int(resp_data[1:-2])
        
        # Standard HLL error rate for m=16384 is ~1.04/sqrt(m) = 0.81%.
        # Let's verify that the estimate is within 2% of the actual 5000.
        error_rate = abs(count - 5000) / 5000
        self.assertLess(error_rate, 0.02, f"HLL estimate {count} is too far from 5000 (error: {error_rate:.2%})")

    def test_pfmerge(self):
        # Create hll1 with {"a", "b", "c"}
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["hll1", "a", "b", "c"])], self.client)
        # Create hll2 with {"c", "d", "e"}
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["hll2", "c", "d", "e"])], self.client)
        
        # Merge hll1 and hll2 into hll3 (non-existing)
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFMERGE", ["hll3", "hll1", "hll2"])], self.client)
        self.assertEqual(self.client.sent_data, b"+OK\r\n")
        
        # Count of hll3 should be 5
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["hll3"])], self.client)
        self.assertEqual(self.client.sent_data, b":5\r\n")
        
        # Merge hll1 into existing hll2
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFMERGE", ["hll2", "hll1"])], self.client)
        self.assertEqual(self.client.sent_data, b"+OK\r\n")
        
        # Count of hll2 should now be 5
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["hll2"])], self.client)
        self.assertEqual(self.client.sent_data, b":5\r\n")

    def test_pfcount_multiple_keys(self):
        # Create hll1 with {"a", "b"}
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["hll1", "a", "b"])], self.client)
        # Create hll2 with {"b", "c"}
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["hll2", "b", "c"])], self.client)
        
        # PFCOUNT on both should return 3 (union of {"a", "b"} and {"b", "c"})
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["hll1", "hll2"])], self.client)
        self.assertEqual(self.client.sent_data, b":3\r\n")

    def test_wrongtype_errors(self):
        # Create a set key
        Store.put("myset", RedisObject(None, REDIS_OBJECT_TYPES.TYPE_SET, REDIS_OBJECT_ENCODINGS.HT), -1, 0)
        
        # PFADD on set key should return WRONGTYPE
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["myset", "foo"])], self.client)
        self.assertTrue(self.client.sent_data.startswith(b"-WRONGTYPE"))
        
        # PFCOUNT on set key should return WRONGTYPE
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFCOUNT", ["myset"])], self.client)
        self.assertTrue(self.client.sent_data.startswith(b"-WRONGTYPE"))
        
        # Create a standard string (not HLL)
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("SET", ["mystr", "hello"])], self.client)
        
        # PFADD on non-HLL string key should return WRONGTYPE
        self.client.sent_data.clear()
        Evaluator.evalAndRespond([RedisCmd("PFADD", ["mystr", "foo"])], self.client)
        self.assertTrue(self.client.sent_data.startswith(b"-WRONGTYPE"))

if __name__ == "__main__":
    unittest.main()
