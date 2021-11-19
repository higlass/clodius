import math

import numpy as np

import clodius.tiles.format as hgfo


def tiles_wrapper(grid, tile_ids):
    tile_values = []

    for tile_id in tile_ids:
        parts = tile_id.split(".")

        if len(parts) < 4:
            raise IndexError("Not enough tile info present")

        z = int(parts[1])
        x = int(parts[3])
        y = int(parts[2])

        ret_array = tiles(grid, z, x, y).reshape((-1))

        tile_values += [(tile_id, hgfo.format_dense_tile(ret_array))]

    return tile_values


def tileset_info(grid, bounds=None):
    """
    Get the tileset info for the grid
    """
    bin_size = 256
    max_dim = max(grid.shape)
    # print("grid.shape:", grid.shape)
    max_zoom = math.ceil(math.log(max_dim / bin_size) / math.log(2))
    max_zoom = 0 if max_zoom < 0 else max_zoom

    max_width = 2 ** max_zoom * bin_size
    max_width1 = 2 ** max_zoom * bin_size

    scale_up = max_width / max_dim

    if bounds is not None:
        min_pos = [bounds[0], bounds[1]]
        max_pos = [bounds[2], bounds[3]]

        # print('scale_up:', scale_up, max_pos[0] - min_pos[0])

        max_width = (max_pos[0] - min_pos[0]) * scale_up
        max_width1 = (max_pos[1] - min_pos[1]) * scale_up
    else:
        min_pos = [0, 0]
        max_pos = grid.shape

    if len(grid.shape) > 2:
        raise ValueError("Grid's shape is not conducive to plotting", grid.shape)
    return {
        "max_width": max_width,
        "max_width1": max_width1,
        "min_pos": min_pos,
        "max_pos": max_pos,
        "max_zoom": max_zoom,
        "mirror_tiles": "false",
        "bins_per_dimension": bin_size,
    }


def tiles(grid, z, x, y, nan_grid=None, bin_size=256):
    """
    Return tiles at the given positions.

    Parameters
    -----------
    grid: np.array
        An nxn array containing values
    z: int
        The zoom level (0 corresponds to most zoomed out)
    x: int
        The x tile position
    y: int
        The y tile position
    bin_size: int
        The number of values per bin
    """
    max_dim = max(grid.shape)

    max_zoom = math.ceil(math.log(max_dim / bin_size) / math.log(2))
    max_zoom = 0 if max_zoom < 0 else max_zoom

    # max_width = 2 ** max_zoom * bin_size

    tile_width = 2 ** (max_zoom - z) * bin_size

    x_start = x * tile_width
    y_start = y * tile_width

    x_end = min(grid.shape[0], x_start + tile_width)
    y_end = min(grid.shape[1], y_start + tile_width)

    num_to_sum = 2 ** (max_zoom - z)

    data = grid[x_start:x_end, y_start:y_end]

    # add some data so that the data can be divided into squares
    # We use max(1, data.shape...) to make avoid the condition where
    # a narrow matrix yields data.shape[0] or data.shape[1] being zero
    # and we return a degenerate tile
    divisible_x_width = num_to_sum * math.ceil(max(1, data.shape[0]) / num_to_sum)
    divisible_y_width = num_to_sum * math.ceil(max(1, data.shape[1]) / num_to_sum)

    divisible_x_pad = divisible_x_width - data.shape[0]
    divisible_y_pad = divisible_y_width - data.shape[1]

    a = np.pad(
        data,
        ((0, divisible_x_pad), (0, divisible_y_pad)),
        "constant",
        constant_values=(np.nan, np.nan),
    )

    b = np.nansum(a.reshape((a.shape[0], -1, num_to_sum)), axis=2)
    ret_array = np.nansum(b.T.reshape(b.shape[1], -1, num_to_sum), axis=2).T
    ret_array[ret_array == 0.0] = np.nan

    if nan_grid is not None:
        # we want to calculate the means of the data points

        # NOTE: In the line below, "nan_grid" was originally "not_nan_grid",
        # which is undefined. This is my best guess of the desired behavior.
        not_nan_data = nan_grid[x_start:x_end, y_start:y_end]
        na = np.pad(
            not_nan_data,
            ((0, divisible_x_pad), (0, divisible_y_pad)),
            "constant",
            constant_values=(np.nan, np.nan),
        )
        nb = np.nansum(na.reshape((na.shape[1], -1, num_to_sum)), axis=2)
        norm_array = np.nansum(nb.T.reshape(nb.shape[1], -1, num_to_sum), axis=2).T

        ret_array = ret_array / norm_array

    # determine how much to pad the array
    x_pad = bin_size - ret_array.shape[0]
    y_pad = bin_size - ret_array.shape[1]

    return np.pad(
        ret_array,
        ((0, x_pad), (0, y_pad)),
        "constant",
        constant_values=(np.nan, np.nan),
    )
