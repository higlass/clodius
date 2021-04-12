import numpy as np

import clodius.tiles.npvector as hgnv


def test_npvector():
    array = np.array([float(f) for f in range(100)])
    # print('ts:', hgnv.tileset_info(array))
    assert "max_width" in hgnv.tileset_info(array)

    hgnv.tiles(array, 0, 0)
    # TODO: Make assertions about tile returned.
