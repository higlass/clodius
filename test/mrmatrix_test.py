import unittest

import numpy as np

from clodius.tiles.mrmatrix import tileset_info, tiles

class TilesetInfoTest(unittest.TestCase):

    def setUp(self):
        # Fake tilesets:
        tileset_stub = {
            'resolutions': {
                '1': {
                    'values': np.array([[1,2],[3,4]])
                }
            }
        }

        class AttrDict(dict):
            pass

        self.tileset = AttrDict(tileset_stub)
        self.tileset.attrs = {}

        self.tileset_min = AttrDict(tileset_stub)
        self.tileset_min.attrs = {'min-pos': (1, 1)}

        self.tileset_max = AttrDict(tileset_stub)
        self.tileset_max.attrs = {'max-pos': (9, 9)}

        self.info = {
            'bins_per_dimension': 256,
            'max_pos': (2, 2),
            'min_pos': [0, 0],
            'mirror_tiles': 'false',
            'resolutions': [1]
        }

    def test_default(self):
        self.assertEqual(tileset_info(self.tileset), self.info)
        self.assertEqual(tileset_info(self.tileset, bounds='???'), self.info)

    def test_with_min(self):
        self.info['min_pos'] = (1, 1)
        self.assertEqual(tileset_info(self.tileset_min), self.info)
        self.assertEqual(tileset_info(self.tileset_min, bounds='???'), self.info)

    def test_with_max(self):
        self.info['max_pos'] = (9, 9)
        self.assertEqual(tileset_info(self.tileset_max), self.info)
        self.assertEqual(tileset_info(self.tileset_max, bounds='???'), self.info)


class TilesTest(unittest.TestCase):
    def test_tiles(self):
        pass
