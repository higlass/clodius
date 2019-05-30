import clodius.array as ct
import math
import numpy as np


def get_tileset_info(hdf_file):
    '''
    Get information about the tileset.

    :param hdf_file: A file handle for an HDF5 file (h5py.File('...'))
    '''
    d = hdf_file['meta']

    if "min-pos" in d.attrs:
        min_pos = d.attrs['min-pos']
    else:
        min_pos = 0

    if "max-pos" in d.attrs:
        max_pos = d.attrs['max-pos']
    else:
        max_pos = d.attrs['max-length']

    return {
        "max_pos": max_pos,
        'min_pos': min_pos,
        "max_width": d.attrs['max-width'],
        "max_zoom": d.attrs['max-zoom'],
        "tile_size": d.attrs['tile-size']
    }


def bisect_left(a, x, lo=0, hi=None, comparator=None):
    '''Bisect_left with with an additional comparator.

    Based on the bisect_left function from the python bisect module.

    Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.

    Args:
        a (array): The array to bisect
        x (object): The object to find the insertion point of
        lo (int): The starting index of items to search in a
        hi (int): The end index of items to search in a
        comparator (function(a,b)): A way to compare objects
    '''

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if comparator(a[mid], x) < 0:
            lo = mid + 1
        else:
            hi = mid
    return lo


def bisect_right(a, x, lo=0, hi=None, comparator=None):
    '''Bisect_right with with an additional comparator.

    Based on the bisect_right function from the python bisect module.

    Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    insert just after the rightmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.


    Args:
        a (array): The array to bisect
        x (object): The object to find the insertion point of
        lo (int): The starting index of items to search in a
        hi (int): The end index of items to search in a
        comparator (function(a,b)): A way to compare objects
    '''
    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if comparator(x, a[mid]) < 0:
            hi = mid
        else:
            lo = mid + 1
    return lo


def get_discrete_data(hdf_file, z, x):
    '''
    Get a discrete set of data from an hdf_tile file.

    Args:
        hdf_file (h5py.File): File handle for the file containing the information
        z (int): The zoom level of this tile
        x (int): The x position of this tile

    Returns:
        A 2D array of entries at that position. It is assumed that names of the
        columns are known.
    '''
    # is the title within the range of possible tiles
    if x > 2**z:
        print("OUT OF RIGHT RANGE")
        return []
    if x < 0:
        print("OUT OF LEFT RANGE")
        return []

    d = hdf_file['meta']
    tile_size = int(d.attrs['tile-size'])
    # max_length = int(d.attrs['max-length'])
    max_zoom = int(d.attrs['max-zoom'])
    # max_width = tile_size * 2 ** max_zoom

    # f is an array of data (e.g. [['34','53', 'x'],['48','57', 'y']] )
    # where the first two columns indicate the start and end points
    f = hdf_file[str(z)]

    # the tile width depends on the zoom level, lower zoom tiles encompass
    # a broader swatch of the data
    tile_width = tile_size * 2 ** (max_zoom - z)
    tile_start = x * tile_width
    tile_end = tile_start + tile_width

    # We need a way to compare data points that aren't numbers
    # (e.g. the arrays that are in f)
    def comparator_start(a, b):
        return int(a[0]) - int(b[0])

    def comparator_end(a, b):
        return int(a[1]) - int(b[1])

    tile_data_start = bisect_left(f, [tile_start], comparator=comparator_start)
    tile_data_end = bisect_right(f, [tile_end], comparator=comparator_start)

    return f[tile_data_start:tile_data_end]


def get_data(hdf_file, z, x):
    '''
    Return a tile from an hdf_file.

    :param hdf_file: A file handle for an HDF5 file (h5py.File('...'))
    :param z: The zoom level
    :param x: The x position of the tile
    '''

    # is the title within the range of possible tiles
    if x > 2**z:
        print("OUT OF RIGHT RANGE")
        return []
    if x < 0:
        print("OUT OF LEFT RANGE")
        return []

    d = hdf_file['meta']

    tile_size = int(d.attrs['tile-size'])
    zoom_step = int(d.attrs['zoom-step'])
    max_zoom = int(d.attrs['max-zoom'])
    max_width = tile_size * 2 ** max_zoom

    if 'max-position' in d.attrs:
        max_position = int(d.attrs['max-position'])
    else:
        max_position = max_width

    rz = max_zoom - z
    # tile_width = max_width / 2**z

    # because we only store some a subsection of the zoom levels
    next_stored_zoom = zoom_step * math.floor(rz / zoom_step)
    zoom_offset = rz - next_stored_zoom

    # the number of entries to aggregate for each new value
    num_to_agg = 2 ** zoom_offset
    total_in_length = tile_size * num_to_agg

    # which positions we need to retrieve in order to dynamically aggregate
    start_pos = int((x * 2 ** zoom_offset * tile_size))
    end_pos = int(start_pos + total_in_length)

    # print("max_position:", max_position)
    max_position = int(max_position / 2 ** next_stored_zoom)
    # print("new max_position:", max_position)

    '''
    print("start_pos:", start_pos)
    print("end_pos:", end_pos)
    print("next_stored_zoom", next_stored_zoom)
    print("max_position:", int(max_position))
    '''

    f = hdf_file['values_' + str(int(next_stored_zoom))]

    if start_pos > max_position:
        # we want a tile that's after the last bit of data
        a = np.zeros(end_pos - start_pos)
        a.fill(np.nan)
        ret_array = ct.aggregate(a, int(num_to_agg))
    elif start_pos < max_position and max_position < end_pos:
        a = f[start_pos:end_pos][:]
        a[max_position + 1:end_pos] = np.nan
        ret_array = ct.aggregate(a, int(num_to_agg))
    else:
        ret_array = ct.aggregate(f[start_pos:end_pos], int(num_to_agg))

    '''
    print("ret_array:", f[start_pos:end_pos])
    print('ret_array:', ret_array)
    '''
    # print('nansum', np.nansum(ret_array))

    # check to see if we counted the number of NaN values in the given
    # interval

    f_nan = None
    if "nan_values_" + str(int(next_stored_zoom)) in hdf_file:
        f_nan = hdf_file['nan_values_' + str(int(next_stored_zoom))]
        nan_array = ct.aggregate(f_nan[start_pos:end_pos], int(num_to_agg))
        num_aggregated = 2 ** (max_zoom - z)

        num_vals_array = np.zeros(len(nan_array))
        num_vals_array.fill(num_aggregated)
        num_summed_array = num_vals_array - nan_array

        averages_array = ret_array / num_summed_array

        return averages_array

    return ret_array
