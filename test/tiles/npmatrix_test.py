import numpy as np

import clodius.tiles.npmatrix as hgnp


def test_numpy_matrix():
    grid = np.array(np.random.rand(100, 100))

    tile = hgnp.tiles(grid, 0, 0, 0)
    assert tile.shape == (256, 256)


def test_numpy_narrow_matrix():
    grid = np.array(np.random.rand(2, 10000))

    # make sure we can fetch a tile that would be empty
    # because of the narrowness of the matrix
    tile = hgnp.tiles(grid, 1, 1, 0)
    assert tile.shape == (256, 256)
