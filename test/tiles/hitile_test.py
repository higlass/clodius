import dask.array as da
import h5py
import clodius.tiles.hitile as hghi
import numpy as np
import os.path as op
import tempfile


def test_hitile():
    array_size = int(1e6)
    chunk_size = 2**19

    data = da.from_array(np.random.random((array_size,)), chunks=(chunk_size,))

    with tempfile.TemporaryDirectory() as td:
        output_file = op.join(td, 'blah.hitile')
        hghi.array_to_hitile(data, output_file, zoom_step=6)

        with h5py.File(output_file, 'r') as f:
            (means, mins, maxs) = hghi.get_data(f, 0, 0)
        # print("means, mins:", means[:10], mins[:10], maxs[:10])
