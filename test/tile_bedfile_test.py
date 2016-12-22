from __future__ import print_function

import clodius.hdf_tiles as cht
import h5py
import pybedtools as pbt

def test_get_tiles():
    f = h5py.File('test/sample_data/cnv.hibed')
    data = cht.get_discrete_data(f, 22, 48)

    assert(len(data) > 0)

    data = cht.get_discrete_data(f, 22, 50)
    assert(len(data) > 0)

    data = cht.get_discrete_data(f, 0, 0)
    assert(len(data) == 100)

def test_tile_ranges():
    f = h5py.File('test/sample_data/cnv.hibed')

    data11 = cht.get_discrete_data(f, 11, 6)
    max_length_11 = max([int(d[2]) - int(d[1]) for d in data11])
    print("data11:", max_length_11)

    data10 = cht.get_discrete_data(f, 10, 3)
    max_length_10 = max([int(d[2]) - int(d[1]) for d in data10])
    print("data10:", max_length_10)

    # more zoomed out tiles should have longer tiles than more
    # zoomed in tiles
    assert(max_length_10 >= max_length_11)


