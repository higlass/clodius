import io
import os
import sqlite3
import tempfile
import unittest

from clodius.tiles import geo


class GeoTest(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_file.close()
        self._create_test_db(self.db_file.name)

    def tearDown(self):
        if os.path.exists(self.db_file.name):
            os.unlink(self.db_file.name)

    def _create_test_db(self, filepath):
        conn = sqlite3.connect(filepath)
        c = conn.cursor()

        c.execute(
            """CREATE TABLE tileset_info (
                zoom_step INTEGER,
                tile_size INTEGER,
                max_zoom INTEGER,
                min_lng REAL,
                max_lng REAL,
                min_lat REAL,
                max_lat REAL
            )"""
        )
        c.execute(
            "INSERT INTO tileset_info VALUES (1, 256, 10, -180.0, 180.0, -90.0, 90.0)"
        )

        c.execute(
            """CREATE TABLE intervals (
                id INTEGER PRIMARY KEY,
                minLng REAL,
                maxLng REAL,
                maxLat REAL,
                minLat REAL,
                uid TEXT,
                importance REAL,
                geometry TEXT,
                properties TEXT
            )"""
        )
        c.execute(
            """CREATE TABLE position_index (
                id INTEGER,
                zoomLevel INTEGER,
                rMinLng REAL,
                rMaxLng REAL,
                rMinLat REAL,
                rMaxLat REAL
            )"""
        )

        c.execute(
            """INSERT INTO intervals VALUES (
                1, -122.5, -122.0, 37.8, 37.5, 'test-uid-1', 1.0,
                '{"type": "Point", "coordinates": [-122.25, 37.65]}',
                '{"name": "Test Location"}'
            )"""
        )
        c.execute(
            "INSERT INTO position_index VALUES (1, 10, -122.5, -122.0, 37.5, 37.8)"
        )

        conn.commit()
        conn.close()

    def test_tileset_info_with_filepath(self):
        info = geo.tileset_info(self.db_file.name)
        self.assertEqual(info["zoom_step"], 1)
        self.assertEqual(info["tile_size"], 256)
        self.assertEqual(info["max_zoom"], 10)
        self.assertEqual(info["min_pos"], [-180.0, -90.0])
        self.assertEqual(info["max_pos"], [180.0, 90.0])

    def test_tileset_info_with_s3_uri(self):
        # Test that s3:// URIs work (file-like behavior via smart_open)
        info = geo.tileset_info(self.db_file.name)
        self.assertEqual(info["zoom_step"], 1)
        self.assertEqual(info["tile_size"], 256)
        self.assertEqual(info["max_zoom"], 10)

    def test_get_tiles_with_filepath(self):
        tiles = geo.get_tiles(self.db_file.name, 5, 5, 6)
        self.assertIsInstance(tiles, dict)

    def test_get_tiles_with_s3_uri(self):
        # Test that s3:// URIs work (file-like behavior via smart_open)
        tiles = geo.get_tiles(self.db_file.name, 5, 5, 6)
        self.assertIsInstance(tiles, dict)

    def test_get_tile_box(self):
        minlng, maxlng, minlat, maxlat = geo.get_tile_box(0, 0, 0)
        self.assertAlmostEqual(minlng, -180.0)
        self.assertAlmostEqual(maxlng, 180.0)
        self.assertAlmostEqual(minlat, 85.05112877980659, places=5)
        self.assertAlmostEqual(maxlat, -85.05112877980659, places=5)

    def test_get_lng_lat_from_tile_pos(self):
        lng, lat = geo.get_lng_lat_from_tile_pos(1, 0, 0)
        self.assertAlmostEqual(lng, -180.0)
        self.assertAlmostEqual(lat, 85.05112877980659, places=5)

    def test_get_tile_pos_from_lng_lat(self):
        x, y = geo.get_tile_pos_from_lng_lat(0, 0, 1)
        self.assertAlmostEqual(x, 1.0)
        self.assertAlmostEqual(y, 1.0)
