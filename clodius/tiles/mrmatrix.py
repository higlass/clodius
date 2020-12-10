import numpy as np

def tileset_info(f, bounds=None):
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

def tiles(f, z,x,y):
    '''
    Return tiles for the given region.

    Parameters:
    -----------
    f: h5py.File
        File pointer to the hdf5 file containing the matrices
    z: int
        The zoom level
    x: int
        The tile's x position
    y: int
        The tile's y position
    '''
    resolutions = sorted(map(int, f['resolutions'].keys()))[::-1]
    tsinfo = tileset_info(f)
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
