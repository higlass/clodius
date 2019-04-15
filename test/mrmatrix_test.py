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

    def test_tileset_info(self):
        # TODO: "bounds" kwarg is is not actually used... Is something missing?

        # Assertions:
        default = {
            'bins_per_dimension': 256,
            'max_pos': (2, 2),
            'min_pos': [0, 0],
            'mirror_tiles': 'false',
            'resolutions': [1]
        }
        self.assertEqual(tileset_info(self.tileset), default)
        self.assertEqual(tileset_info(self.tileset, bounds='???'), default)

        with_min = {
            'bins_per_dimension': 256,
            'max_pos': (2, 2),
            'min_pos': (1, 1),
            'mirror_tiles': 'false',
            'resolutions': [1]
        }
        self.assertEqual(tileset_info(self.tileset_min), with_min)
        self.assertEqual(tileset_info(self.tileset_min, bounds='???'), with_min)

        with_max = {
            'bins_per_dimension': 256,
            'max_pos': (9, 9),
            'min_pos': [0, 0],
            'mirror_tiles': 'false',
            'resolutions': [1]
        }
        self.assertEqual(tileset_info(self.tileset_max), with_max)
        self.assertEqual(tileset_info(self.tileset_max, bounds='???'), with_max)

class TilesTest(unittest.TestCase):
    def test_tiles(self):
        pass
