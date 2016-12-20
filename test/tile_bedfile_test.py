from __future__ import print_function

import clodius.hdf_tiles as cht
import h5py
import pybedtools as pbt

def test_get_tiles():
    f = h5py.File('test/sample_data/cnv.hibed')
    cht.get_discrete_data(f, 22, 48)

    cht.get_discrete_data(f, 22, 50)


