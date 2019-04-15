import unittest

import numpy as np
from numpy.testing import assert_array_equal

from clodius.tiles.mrmatrix import tileset_info, tiles

class AttrDict(dict):
    pass

class TilesetInfoTest(unittest.TestCase):

    def setUp(self):
        tileset_stub = {
            'resolutions': {
                '1': {
                    'values': np.array([[1,2],[3,4]])
                }
            }
        }

        self.tileset = AttrDict(tileset_stub)
        self.tileset.attrs = {}

        self.tileset_min = AttrDict(tileset_stub)
        self.tileset_min.attrs = {'min-pos': (1, 1)}

        self.tileset_max = AttrDict(tileset_stub)
        self.tileset_max.attrs = {'max-pos': (9, 9)}

        self.info = {
            'bins_per_dimension': 256,
            'max_pos': (2, 2),       # TODO: Nothing uses these...
            'min_pos': [0, 0],       # ...
            'mirror_tiles': 'false', # Can we remove them?
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
    def test_zoom_out_of_bounds(self):
        def should_fail():
            tileset_stub = AttrDict({
                'resolutions': {
                    '1': {
                        'values': np.array([[1,2],[3,4]])
                    }
                }
            })
            tileset_stub.attrs = {}
            tiles(tileset_stub, 2, 0, 0)
        self.assertRaisesRegex(ValueError, r'Zoom level out of bounds', should_fail)

    def test_padding(self):
        tileset = AttrDict({
            'resolutions': {
                '1': {
                    'values': np.array([[1.0, 2], [3, 4]])
                    # It's important that there is a float value:
                    # If there isn't, np.nan will be converted to a large negative integer.
                }
            }
        })
        tileset.attrs = {}
        zoomed = tiles(tileset, 0, 0, 0)
        self.assertEqual(zoomed.shape, (256, 256))
        assert_array_equal(zoomed[0:2, 0:2], [[1, 2], [3, 4]])
        assert_array_equal(zoomed[2:256, 0], [np.nan for x in range(254)])

    def test_bins(self):
        tileset = AttrDict({
            'resolutions': {
                '1': {
                    'values': np.array([[float(x) for x in range(500)] for y in range(500)])
                }
            }
        })
        tileset.attrs = {}

        zoomed_0 = tiles(tileset, 0, 0, 0)
        self.assertEqual(zoomed_0.shape, (256, 256))
        self.assertEqual(zoomed_0[0, 0], 0)

        zoomed_1 = tiles(tileset, 0, 1, 1)
        self.assertEqual(zoomed_1.shape, (256, 256))
        self.assertEqual(zoomed_1[0, 0], 256)
        self.assertEqual(zoomed_1[1, 0], 256) # Constant dimension
        self.assertEqual(zoomed_1[0, 1], 257) # Changing dimension
        self.assertEqual(zoomed_1[0, 256 - 13], 499)
        assert_array_equal(zoomed_1[0:1, (256 - 12):(256 - 12 + 1)], [[np.nan]])
        # Plain assertEqual gave: nan != nan

    def test_zoom(self):
        tileset = AttrDict({
            'resolutions': {
                # TODO: It's not actually enforced that zoom levels be sequential integers?
                # TODO: Should we check that the sizes are reasonable during initialization?
                '1': {
                    'values': np.array([[1,2],[3,4]])
                },
                '5': {
                    'values': np.array([[3,4],[5,6]])
                },
                '11': {
                    'values': np.array([[5,6],[7,8]])
                }
            }
        })
        tileset.attrs = {}

        zoomed_0 = tiles(tileset, 0, 0, 0)
        assert_array_equal(zoomed_0[0:2, 0:2], [[5, 6], [7, 8]])

        zoomed_1 = tiles(tileset, 1, 0, 0)
        assert_array_equal(zoomed_1[0:2, 0:2], [[3, 4], [5, 6]])

        zoomed_2 = tiles(tileset, 2, 0, 0)
        assert_array_equal(zoomed_2[0:2, 0:2], [[1, 2], [3, 4]])
