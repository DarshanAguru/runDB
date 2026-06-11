import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.encoding import Encoder
from core.RedisObject import REDIS_OBJECT_TYPES, REDIS_OBJECT_ENCODINGS

class TestEncoder(unittest.TestCase):
    def test_encode_integer(self):
        self.assertEqual(Encoder.encode(42), b":42\r\n")
        self.assertEqual(Encoder.encode(-10), b":-10\r\n")

    def test_encode_simple_string(self):
        self.assertEqual(Encoder.encode("OK"), b"+OK\r\n")

    def test_encode_bulk_string(self):
        self.assertEqual(Encoder.encode("hello", bulk=True), b"$5\r\nhello\r\n")

    def test_encode_empty_array(self):
        self.assertEqual(Encoder.encode([]), b"+(empty array)\r\n")

    def test_encode_array(self):
        # Array items encode as bulk strings
        self.assertEqual(Encoder.encode(["SET", "key", 42]), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n:42\r\n")

    def test_deduce_type_encoding(self):
        # Integers
        t, e = Encoder.deduceTypeEncoding("12345")
        self.assertEqual(t, REDIS_OBJECT_TYPES.TYPE_STRING)
        self.assertEqual(e, REDIS_OBJECT_ENCODINGS.INT)

        # Embedded strings (length <= 44)
        t, e = Encoder.deduceTypeEncoding("a" * 44)
        self.assertEqual(t, REDIS_OBJECT_TYPES.TYPE_STRING)
        self.assertEqual(e, REDIS_OBJECT_ENCODINGS.EMBSTR)

        # Raw strings (length > 44)
        t, e = Encoder.deduceTypeEncoding("a" * 45)
        self.assertEqual(t, REDIS_OBJECT_TYPES.TYPE_STRING)
        self.assertEqual(e, REDIS_OBJECT_ENCODINGS.RAW)

if __name__ == "__main__":
    unittest.main()
