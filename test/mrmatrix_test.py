import unittest

import numpy as np

from clodius.tiles.mrmatrix import tileset_info, tiles

class AttrDict(dict):
    pass

tileset_stub = {
    'resolutions': {
        '1': {
            'values': np.array([[1,2],[3,4]])
        }
    }
}

tileset = AttrDict(tileset_stub)
tileset.attrs = {}

tileset_min = AttrDict(tileset_stub)
tileset_min.attrs = {'min-pos': (1, 1)}

tileset_max = AttrDict(tileset_stub)
tileset_max.attrs = {'max-pos': (9, 9)}

class MrMatrixTest(unittest.TestCase):

    def test_tileset_info(self):
        # TODO: "bounds" kwarg is is not actually used... Is something missing?

        default = {
            'bins_per_dimension': 256,
            'max_pos': (2, 2),
            'min_pos': [0, 0],
            'mirror_tiles': 'false',
            'resolutions': [1]
        }
        self.assertEqual(tileset_info(tileset), default)
        self.assertEqual(tileset_info(tileset, bounds='???'), default)

        with_min = {
            'bins_per_dimension': 256,
            'max_pos': (2, 2),
            'min_pos': (1, 1),
            'mirror_tiles': 'false',
            'resolutions': [1]
        }
        self.assertEqual(tileset_info(tileset_min), with_min)
        self.assertEqual(tileset_info(tileset_min, bounds='???'), with_min)

        with_max = {
            'bins_per_dimension': 256,
            'max_pos': (9, 9),
            'min_pos': [0, 0],
            'mirror_tiles': 'false',
            'resolutions': [1]
        }
        self.assertEqual(tileset_info(tileset_max), with_max)
        self.assertEqual(tileset_info(tileset_max, bounds='???'), with_max)
