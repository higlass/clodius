from __future__ import print_function

import clodius.hdf_tiles as ch
import h5py
import sys

sys.path.append("scripts")

def test_1d_tile():
    f = h5py.File('test/sample_data/test.tile_generation.hdf5')

    max_zoom = f['meta'].attrs['max-zoom']
    tile_size = f['meta'].attrs['tile-size']

    print("max_zoom:", max_zoom)

    d = ch.get_data(f, max_zoom, 0)

    # lowest zoom should have values of 1
    for i in range(tile_size):
        assert(d[i] == 1)

    d = ch.get_data(f, max_zoom-1, 0)
    print("d:", d)

    for i in range(tile_size):
        assert(d[i] == 2)

    d = ch.get_data(f, max_zoom-2, 0)
    print("d:", d)

    for i in range(tile_size / 2):
        assert(d[i] == 4)

    print("d[511]:", d[511])
    print("d[512]:", d[512])
    print("d[513]:", d[513])
    assert(d[513] == 4)
