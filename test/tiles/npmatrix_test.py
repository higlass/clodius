import clodius.tiles.npmatrix as hgnp
import numpy as np


def test_numpy_matrix():
    grid = np.array(np.random.rand(100, 100))
    # print('grid:', grid)

    tile = hgnp.tiles(grid, 0, 0, 0)
    assert tile.shape == (256, 256)
