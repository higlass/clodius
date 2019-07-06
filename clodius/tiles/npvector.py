import math
import numpy as np
import clodius.tiles.format as ctf


def tiles_wrapper(array, tile_ids, not_nan_array=None):
    tile_values = []

    for tile_id in tile_ids:
        parts = tile_id.split('.')

        if len(parts) < 3:
            raise IndexError("Not enough tile info present")

        z = int(parts[1])
        x = int(parts[2])

        ret_array = tiles(array, z, x, not_nan_array).reshape((-1))

        tile_values += [(tile_id, ctf.format_dense_tile(ret_array))]

    return tile_values


def tileset_info(array, bounds=None, bins_per_dimension=1024):
    '''
    Get the tileset info for the array
    '''
    max_dim = max(array.shape)

    max_zoom = math.ceil(math.log(max_dim / bins_per_dimension) / math.log(2))
    max_zoom = 0 if max_zoom < 0 else max_zoom

    max_width = 2 ** max_zoom * bins_per_dimension
    # print('max_zoom:', max_zoom)

    scale_up = max_width / max_dim

    if bounds:
        min_pos = [bounds[0]]
        max_pos = [bounds[1]]

        # print('scale_up:', scale_up, max_pos[0] - min_pos[0])

        max_width = (max_pos[0] - min_pos[0]) * scale_up
    else:
        min_pos = [0]
        max_pos = [array.shape[0]]

    if len(array.shape) > 1:
        raise ValueError("The array shape is not a vector type", array.shape)
    return {
        "max_width": max_width,
        "min_pos": min_pos,
        "max_pos": max_pos,
        "max_zoom": max_zoom,
        "bins_per_dimension": bins_per_dimension
    }


def max_zoom_and_data_bounds(array, z, x, bin_size):
    '''
    Return the maximum zoom level and data corresponding to the
    zoom level and position of the array.

    Parameters
    ----------
    array: np.array
        The array containing the raw numpy data
    z: int
        The zoom level
    x: int
        The x position
    '''
    max_dim = array.shape[0]

    max_zoom = math.ceil(math.log(max_dim / bin_size) / math.log(2))
    max_zoom = 0 if max_zoom < 0 else max_zoom

    # max_width = 2 ** max_zoom * bin_size
    # print("max_width:", max_width, 'bin_size:', bin_size, 'max_zoom', max_zoom)

    tile_width = 2 ** (max_zoom - z) * bin_size

    x_start = x * tile_width
    x_end = min(array.shape[0], x_start + tile_width)

    # print("x_start:", x_start, x_end)

    return max_zoom, x_start, x_end


def tiles(array, z, x, not_nan_array=None, bin_size=1024):
    '''
    Return tiles at the given positions.

    Parameters
    -----------
    array: np.array
        An nxn array containing values
    z: int
        The zoom level (0 corresponds to most zoomed out)
    x: int
        The x tile position
    not_nan_array: np.array
        An array storing the number of values which are not nan
        in the original array. Can be precalculated for speed.
    bin_size: int
        The number of values per bin
    '''
    max_zoom, x_start, x_end = max_zoom_and_data_bounds(array, z, x, bin_size)
    data = array[x_start:x_end]

    num_to_sum = 2 ** (max_zoom - z)

    # add some data so that the data can be divided into squares
    divisible_x_width = num_to_sum * math.ceil(data.shape[0] / num_to_sum)
    divisible_x_pad = divisible_x_width - data.shape[0]

    a = np.pad(data, ((0, divisible_x_pad),), 'constant',
               constant_values=(np.nan,))

    ret_array = np.nansum(a.reshape((-1, num_to_sum)), axis=1)

    if not_nan_array is None:
        not_nan_data = ~np.isnan(array[x_start:x_end])
    else:
        not_nan_data = not_nan_array[x_start:x_end]

    # we want to calculate the means of the data points
    na = np.pad(
        not_nan_data,
        ((0, divisible_x_pad)),
        'constant',
        constant_values=(np.nan,)
    )
    norm_array = np.nansum(na.reshape((-1, num_to_sum)), axis=1)
    ret_array = ret_array / (norm_array + 1)

    # determine how much to pad the array
    x_pad = bin_size - ret_array.shape[0]

    return np.pad(
        ret_array, ((0, x_pad)), 'constant', constant_values=(np.nan, )
    )
