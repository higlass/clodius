from __future__ import print_function

import clodius.hdf_tiles as ch
import h5py
import sys

sys.path.append("scripts")

def test_1d_tile():
    check_1d_file('test/sample_data/test.tile_generation.hdf5')

def check_1d_file(filename):
    f = h5py.File(filename)

    max_zoom = f['meta'].attrs['max-zoom']
    tile_size = int(f['meta'].attrs['tile-size'])

    d = ch.get_data(f, max_zoom, 0)

    # lowest zoom should have values of 1
    for i in range(tile_size):
        assert(d[i] == 1)

    d = ch.get_data(f, max_zoom-1, 0)

    for i in range(tile_size):
        assert(d[i] == 2)

    d = ch.get_data(f, max_zoom-2, 0)

    for i in range(tile_size // 2):
        assert(d[i] == 4)

    assert(d[513] == 4)

def test_tileset_info():
    check_tileset_info('test/sample_data/test.tile_generation.hdf5')

def check_tileset_info(filename):
    f = h5py.File(filename)

    ti = ch.get_tileset_info(f)
