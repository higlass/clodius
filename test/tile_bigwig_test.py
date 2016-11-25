from __future__ import print_function

import clodius.hdf_tiles as ch
import h5py
import sys

sys.path.append("scripts")

def test_1d_tile():
    f = h5py.File('test/sample_data/test.tile_generation.hdf5')

    max_zoom = f['meta'].attrs['max-zoom']
    tile_size = f['meta'].attrs['tile-size']

    d = ch.get_data(f, max_zoom, 0)

    # lowest zoom should have values of 1
    for i in range(tile_size):
        assert(d[i] == 1)

    d = ch.get_data(f, max_zoom-1, 0)

    for i in range(tile_size):
        assert(d[i] == 2)

    d = ch.get_data(f, max_zoom-2, 0)

    for i in range(tile_size / 2):
        assert(d[i] == 4)

    assert(d[513] == 4)

def test_tileset_info():
    f = h5py.File('test/sample_data/test.tile_generation.hdf5')

    ti = ch.get_tileset_info(f)
