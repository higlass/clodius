import numpy as np
import h5py
from clodius.tiles.utils import tiles_wrapper_2d
from clodius.tiles.format import format_dense_tile

def tileset_info(file, bounds=None):
    if isinstance(file, (str, bytes)) or hasattr(file, '__fspath__'):
        f = h5py.File(file, "r")
    else:
        # Already an h5py-like object or mock
        f = file

    if 'min-pos' in f.attrs:
        min_pos = f.attrs['min-pos']
    else:
        min_pos = [0,0]

    if 'max-pos' in f.attrs:
        max_pos = f.attrs['max-pos']
    else:
        max_pos = f['resolutions']['1']['values'].shape

    return {
        'min_pos': min_pos,
        'max_pos': max_pos,
        'resolutions': [int(r) for r in f['resolutions']],
        'mirror_tiles': 'false',
        'bins_per_dimension': 256,
    }

def single_tile(file, z,x,y):
    '''
    Return tiles for the given region.

    Parameters:
    -----------
    file: str | filelike
        Path or file-like object of the file to load
    z: int
        The zoom level
    x: int
        The tile's x position
    y: int
        The tile's y position
    '''
    if isinstance(file, (str, bytes)) or hasattr(file, '__fspath__'):
        f = h5py.File(file, "r")
    else:
        # Already an h5py-like object or mock
        f = file

    resolutions = sorted(map(int, f['resolutions'].keys()))[::-1]
    tsinfo = tileset_info(file)
    n_bins = tsinfo['bins_per_dimension']

    if z >= len(resolutions):
        raise ValueError('Zoom level out of bounds:', z,
            "resolutions:", resolutions)

    tile_width = tsinfo['bins_per_dimension']

    # Where in the matrix the tile starts
    tile_x_start = x * tile_width
    tile_y_start = y * tile_width

    tile_x_end = tile_x_start + n_bins
    tile_y_end = tile_y_start + n_bins

    mat = f['resolutions'][str(resolutions[z])]['values']
    data = mat[tile_y_start:tile_y_end,
        tile_x_start:tile_x_end]

    x_pad = n_bins - data.shape[0]
    y_pad = n_bins - data.shape[1]

    if x_pad > 0 or y_pad > 0:
        data = np.pad(data, ((0, x_pad), (0, y_pad)), 'constant',
            constant_values = (np.nan, np.nan))

    return data

def tiles(filepath, tile_ids):
    "Retrieve a set of tiles."
    return tiles_wrapper_2d(
            tile_ids, lambda z, x, y: format_dense_tile(single_tile(filepath, z, x, y))
        )