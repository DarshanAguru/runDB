import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.resp import RESPProcessor

class TestRESPProcessor(unittest.TestCase):
    def test_decode_simple_string(self):
        data = b"+OK\r\n"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(decoded, ["OK"])
        self.assertEqual(consumed, len(data))

    def test_decode_error(self):
        data = b"-ERR something went wrong\r\n"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0], Exception)
        self.assertEqual(str(decoded[0]), "ERR something went wrong")
        self.assertEqual(consumed, len(data))

    def test_decode_integer(self):
        data = b":1000\r\n"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(decoded, [1000])
        self.assertEqual(consumed, len(data))

    def test_decode_bulk_string(self):
        data = b"$5\r\nhello\r\n"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(decoded, ["hello"])
        self.assertEqual(consumed, len(data))

    def test_decode_array(self):
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(decoded, [["SET", "key", "value"]])
        self.assertEqual(consumed, len(data))

    def test_decode_partial(self):
        # Incomplete bulk string
        data = b"$5\r\nhel"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(decoded, [])
        self.assertEqual(consumed, 0)

    def test_decode_pipelined(self):
        data = b"+PING\r\n+PONG\r\n"
        decoded, consumed, err = RESPProcessor.decode(data)
        self.assertIsNone(err)
        self.assertEqual(decoded, ["PING", "PONG"])
        self.assertEqual(consumed, len(data))

if __name__ == "__main__":
    unittest.main()
