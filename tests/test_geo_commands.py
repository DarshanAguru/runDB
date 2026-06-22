import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.evaluator import Evaluator
from core.RedisCmd import RedisCmd
from core.Store import Store

class MockClient:
    def __init__(self):
        self.db = 0
        self.sent_data = bytearray()

    def send(self, data: bytes) -> int:
        self.sent_data.extend(data)
        return len(data)

class TestGeoCommands(unittest.TestCase):
    def setUp(self):
        for db in range(len(Store.stores)):
            Store.stores[db].clear()
            Store.expires_list[db].clear()
        self.client = MockClient()

    def test_geoadd(self):
        # 1. Add members to a new geo key
        cmd = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"])
        Evaluator.evalAndRespond([cmd], self.client)
        self.assertEqual(self.client.sent_data, b":2\r\n")

        # 2. Add duplicate member (updates, returns 0 added)
        self.client.sent_data.clear()
        cmd_dup = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo"])
        Evaluator.evalAndRespond([cmd_dup], self.client)
        self.assertEqual(self.client.sent_data, b":0\r\n")

        # 3. Add invalid coordinates
        self.client.sent_data.clear()
        cmd_invalid = RedisCmd("GEOADD", ["Sicily", "195.0", "38.115556", "InvalidMember"])
        Evaluator.evalAndRespond([cmd_invalid], self.client)
        self.assertTrue(self.client.sent_data.startswith(b"-ERR invalid longitude,latitude pair"))

    def test_geopos(self):
        # Add members
        cmd_add = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"])
        Evaluator.evalAndRespond([cmd_add], self.client)
        self.client.sent_data.clear()

        # Get positions of existing and non-existing members
        cmd_pos = RedisCmd("GEOPOS", ["Sicily", "Palermo", "NonExisting", "Catania"])
        Evaluator.evalAndRespond([cmd_pos], self.client)
        
        # Structure should be an array of 3 elements:
        # 1. Palermo coordinates [lon, lat]
        # 2. Nil for NonExisting
        # 3. Catania coordinates [lon, lat]
        expected_start = b"*3\r\n"
        self.assertTrue(self.client.sent_data.startswith(expected_start))
        
        # Verify Nil is present in the response
        self.assertIn(b"$-1\r\n", self.client.sent_data)

    def test_geodist(self):
        # Add members
        cmd_add = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"])
        Evaluator.evalAndRespond([cmd_add], self.client)
        self.client.sent_data.clear()

        # Distance in km
        cmd_dist_km = RedisCmd("GEODIST", ["Sicily", "Palermo", "Catania", "km"])
        Evaluator.evalAndRespond([cmd_dist_km], self.client)
        # Palermo to Catania is approx 166.27 km
        self.assertTrue(b"166." in self.client.sent_data)

        # Distance with missing unit (defaults to meters)
        self.client.sent_data.clear()
        cmd_dist_m = RedisCmd("GEODIST", ["Sicily", "Palermo", "Catania"])
        Evaluator.evalAndRespond([cmd_dist_m], self.client)
        self.assertTrue(b"16627" in self.client.sent_data)

    def test_geosearch_byradius(self):
        # Add members
        cmd_add = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"])
        Evaluator.evalAndRespond([cmd_add], self.client)
        self.client.sent_data.clear()

        # Find elements within 200 km of longitude 15, latitude 37.5
        cmd_search = RedisCmd("GEOSEARCH", ["Sicily", "FROMLONLAT", "15.0", "37.5", "BYRADIUS", "200", "km", "ASC"])
        Evaluator.evalAndRespond([cmd_search], self.client)
        # Catania is closer to (15, 37.5) than Palermo
        # Order should be Catania, then Palermo
        self.assertIn(b"Catania", self.client.sent_data)
        self.assertIn(b"Palermo", self.client.sent_data)

    def test_geosearch_byradius_options(self):
        cmd_add = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo"])
        Evaluator.evalAndRespond([cmd_add], self.client)
        self.client.sent_data.clear()

        # With distance, coordinates, and hash
        cmd_opt = RedisCmd("GEOSEARCH", ["Sicily", "FROMLONLAT", "13.0", "38.0", "BYRADIUS", "100", "km", "WITHDIST", "WITHCOORD", "WITHHASH"])
        Evaluator.evalAndRespond([cmd_opt], self.client)
        self.assertIn(b"Palermo", self.client.sent_data)
        self.assertIn(b"13.361389", self.client.sent_data)
        self.assertIn(b"38.115556", self.client.sent_data)

    def test_geosearch_bybox(self):
        cmd_add = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"])
        Evaluator.evalAndRespond([cmd_add], self.client)
        self.client.sent_data.clear()

        # Search using BYBOX: width 400km, height 400km centered at Palermo
        cmd_box = RedisCmd("GEOSEARCH", ["Sicily", "FROMMEMBER", "Palermo", "BYBOX", "400", "400", "km", "ASC"])
        Evaluator.evalAndRespond([cmd_box], self.client)
        # Palermo is at the center (0 dist), Catania (approx 166.27 km away)
        # If Catania is within the box, it should be returned.
        # Let's verify Catania is returned.
        self.assertIn(b"Palermo", self.client.sent_data)
        self.assertIn(b"Catania", self.client.sent_data)

    def test_geohash(self):
        cmd_add = RedisCmd("GEOADD", ["Sicily", "13.361389", "38.115556", "Palermo"])
        Evaluator.evalAndRespond([cmd_add], self.client)
        self.client.sent_data.clear()

        cmd_hash = RedisCmd("GEOHASH", ["Sicily", "Palermo"])
        Evaluator.evalAndRespond([cmd_hash], self.client)
        # Palermo geohash is sqc8b49rny0
        self.assertIn(b"sqc8b49rny0", self.client.sent_data)

if __name__ == "__main__":
    unittest.main()
