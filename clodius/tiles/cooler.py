import base64
import collections as col
import cooler
import clodius.tiles.format as hgfo
import clodius.tiles.utils as hgut
import h5py
import itertools as it
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

global mats
mats = {}

transform_descriptions = {}
transform_descriptions['weight'] = {'name': 'ICE', 'value': 'weight'}
transform_descriptions['KR'] = {'name': 'KR', 'value': 'KR'}
transform_descriptions['VC'] = {'name': 'VC', 'value': 'VC'}
transform_descriptions['VC_SQRT'] = {'name': 'VC_SQRT', 'value': 'VC_SQRT'}

TILE_SIZE = 256

def abs_coord_2_bin(c, abs_pos, chroms, chrom_cum_lengths, chrom_sizes):
    """Get bin ID from absolute coordinates.

    Args:
        c (Cooler): Cooler instance of a .cool file.
        abs_pos (int): Absolute coordinate to be translated.

    Returns:
        int: Bin number.
    """

    try:
        chr_id = np.flatnonzero(chrom_cum_lengths > abs_pos)[0] - 1
    except IndexError:
        return c.info['nbins']

    chrom = chroms[chr_id]
    rel_pos = abs_pos - chrom_cum_lengths[chr_id]

    return c.offset((chrom, rel_pos, chrom_sizes[chrom]))


def get_chromosome_names_cumul_lengths(c):
    '''
    Get the chromosome names and cumulative lengths:

    Args:

    c (Cooler): A cooler file

    Return:

    (names, sizes, lengths) -> (list(string), dict, np.array(int))
    '''
    chrom_names = c.chromnames
    chrom_sizes = dict(c.chromsizes)
    chrom_cum_lengths = np.r_[0, np.cumsum(c.chromsizes.values)]
    return chrom_names, chrom_sizes, chrom_cum_lengths


def get_data(f, start_pos_1, end_pos_1, start_pos_2, end_pos_2, transform='default', resolution=None):
    """Get balanced pixel data.

    Args:
        f: h5py.File
            An HDF5 Group that contains the cooler for this resolution
        start_pos_1 (int): Test.
        end_pos_1 (int): Test.
        start_pos_2 (int): Test.
        end_pos_2 (int): Test.

    Returns:
        DataFrame: Annotated cooler pixels.
    """

    c = cooler.Cooler(f)

    (chroms, chrom_sizes, chrom_cum_lengths) = get_chromosome_names_cumul_lengths(c)

    i0 = abs_coord_2_bin(c, start_pos_1, chroms, chrom_cum_lengths, chrom_sizes)
    i1 = abs_coord_2_bin(c, end_pos_1, chroms, chrom_cum_lengths, chrom_sizes)

    j0 = abs_coord_2_bin(c, start_pos_2, chroms, chrom_cum_lengths, chrom_sizes)
    j1 = abs_coord_2_bin(c, end_pos_2, chroms, chrom_cum_lengths, chrom_sizes)

    matrix = c.matrix(as_pixels=True, balance=False, max_chunk=np.inf)

    if i0 >= matrix.shape[0] or j0 >= matrix.shape[1]:
        # query beyond the bounds of the matrix
        # return an empty matrix
        i0,i1,j0,j1 = 0,0,0,0

        return (pd.DataFrame(columns=['genome_start1', 'genome_start2', 'balanced']),
                (pd.DataFrame({'genome_start': [],
                               'genome_end': [],
                               'weight': []}),
                pd.DataFrame({'genome_start': [],
                               'genome_end': [],
                               'weight': []})))
    else:
        # limit the range of the query to be within bounds
        i1 = min(i1, matrix.shape[0]-1)
        j1 = min(j1, matrix.shape[1]-1)

    pixels = matrix[i0:i1+1, j0:j1+1]

    '''
    if not len(pixels):
        return (pd.DataFrame(columns=['genome_start1', 'genome_start2', 'balanced']), (None, None))
    '''

    # select bin columns to extract
    cols = ['chrom', 'start', 'end']
    if (transform == 'default' and 'weight' in c.bins()) or transform == 'weight':
        cols.append('weight')
    elif transform in ('KR', 'VC', 'VC_SQRT'):
        cols.append(transform)

    bins = c.bins(convert_enum=False)[cols]
    pixels = cooler.annotate(pixels, bins)

    pixels['genome_start1'] = chrom_cum_lengths[pixels['chrom1']] + pixels['start1']
    pixels['genome_start2'] = chrom_cum_lengths[pixels['chrom2']] + pixels['start2']

    bins1 = bins[i0:i1+1]
    bins2 = bins[j0:j1+1]


    bins1['genome_start'] = chrom_cum_lengths[bins1['chrom']] + bins1['start']
    bins2['genome_start'] = chrom_cum_lengths[bins2['chrom']] + bins2['start']

    bins1['genome_end'] = chrom_cum_lengths[bins1['chrom']] + bins1['end']
    bins2['genome_end'] = chrom_cum_lengths[bins2['chrom']] + bins2['end']

    # apply transform
    if (transform == 'default' and 'weight' in c.bins()) or transform == 'weight':
        pixels['balanced'] = (
            pixels['count'] * pixels['weight1'] * pixels['weight2']
        )

        return (pixels[['genome_start1', 'genome_start2', 'balanced']], (bins1, bins2))
    elif transform in ('KR', 'VC', 'VC_SQRT'):
        pixels['balanced'] = (
            pixels['count'] / pixels[transform+'1'] / pixels[transform+'2']
        )

        bins1['weight'] = bins1[transform]
        bins2['weight'] = bins2[transform]

        return (pixels[['genome_start1', 'genome_start2', 'balanced']], (bins1, bins2))
    else:
        return (pixels[['genome_start1', 'genome_start2', 'count']], (None, None))


def get_info(file_path):
    """Get information of a cooler file.

    Args:
        file_path (str): Path to a cooler file.

    Returns:
        dict: Dictionary containing basic information about the cooler file.
    """

    with h5py.File(file_path, 'r') as f:
        max_zoom = f.attrs.get('max-zoom')

        if max_zoom is None:
            logger.info('no zoom found')
            raise ValueError(
                'The `max_zoom` attribute is missing.'
            )

        c = cooler.Cooler(f["0"])

        (chroms, chrom_sizes, chrom_cum_lengths) = get_chromosome_names_cumul_lengths(c)

        total_length = int(chrom_cum_lengths[-1])
        max_zoom = f.attrs['max-zoom']
        bin_size = int(f[str(max_zoom)].attrs['bin-size'])

        max_width = bin_size * TILE_SIZE * 2**max_zoom

        # the list of available data transforms
        transforms = {}

        for i in range(max_zoom):
            f_for_zoom = f[str(i)]['bins']

            if 'weight' in f_for_zoom:
                transforms['weight'] = {'name': 'ICE', 'value': 'weight'}
            if 'KR' in f_for_zoom:
                transforms['KR'] = {'name': 'KR', 'value': 'KR'}
            if 'VC' in f_for_zoom:
                transforms['VC'] = {'name': 'VC', 'value': 'VC'}
            if 'VC_SQRT' in f_for_zoom:
                transforms['VC_SQRT'] = {'name': 'VC_SQRT', 'value': 'VC_SQRT'}

        info = {
            'min_pos': [0.0, 0.0],
            'max_pos': [total_length, total_length],
            'max_zoom': max_zoom,
            'max_width': max_width,
            'bins_per_dimension': TILE_SIZE,
            'transforms': transforms.values()
        }

    return info


def get_quadtree_depth(chromsizes, binsize):
    """
    Depth of quad tree necessary to tesselate the concatenated genome with quad
    tiles such that linear dimension of the tiles is a preset multiple of the
    genomic resolution.

    """
    tile_size_bp = TILE_SIZE * binsize
    min_tile_cover = np.ceil(sum(chromsizes) / tile_size_bp)
    return int(np.ceil(np.log2(min_tile_cover)))


def get_zoom_resolutions(chromsizes, base_res):
    return [base_res * 2**x for x in range(get_quadtree_depth(chromsizes, base_res)+1)]


def print_zoom_resolutions(chromsizes_file, base_res):
    """
    Print comma-separated list of zoom resolutions for a given genome
    and base resolution.

    """
    chromsizes = cooler.util.read_chromsizes(chromsizes_file, all_names=True)
    resolutions = get_zoom_resolutions(chromsizes, base_res)
    print(','.join(str(res) for res in resolutions))

def make_tiles(hdf_for_resolution, resolution, x_pos, y_pos, transform_type='default', x_width=1, y_width=1):
    '''
    Generate tiles for a given location. This function retrieves tiles for
    a rectangular region of width x_width and height y_width

    Parameters
    ---------
    hdf_for_resolution: h5py.File
        An HDF group containing the cooler for the given resolution
    x_pos: int
        The starting x position
    y_pos: int
        The starting y position
    cooler_file: string
        The filename of the cooler file to get the data from
    x_width: int
        The number of tiles to retrieve along the x dimension
    y_width: int
        The number of tiles to retrieve along the y dimension

    Returns
    -------
    data_by_tilepos: {(x_pos, y_pos) : np.array}
        A dictionary of tile data indexed by tile positions
    '''
    BINS_PER_TILE = 256

    tile_size = resolution * BINS_PER_TILE

    start1 = x_pos * tile_size
    end1 = (x_pos + x_width) * tile_size
    start2 = y_pos * tile_size
    end2 = (y_pos + y_width) * tile_size

    #print("resolution:", resolution)
    #print("tile_size:", tile_size)
    #print("transform_type:", transform_type);
    #print('start1:', start1, end1)
    #print('start2:', start2, end2)

    c = cooler.Cooler(hdf_for_resolution)
    (chroms, chrom_sizes, chrom_cum_lengths) = get_chromosome_names_cumul_lengths(c)

    total_length = sum(chrom_sizes.values())

    (data, (bins1, bins2)) = get_data(
        hdf_for_resolution, start1, end1 - 1, start2, end2- 1,
        transform_type, resolution=resolution
    )


    #print('start1', start1, 'end1', end1, 'weight', len(weight1), 'end1 - start1 / tile_size', (end1 - start1) / resolution)

    #print("data:", data)

    # print("x_width:", x_width)
    # print("y_width:", y_width)
    # split out the individual tiles
    data_by_tilepos = {}

    for x_offset in range(0, x_width):
        for y_offset in range(0, y_width):

            start1 = (x_pos + x_offset) * tile_size
            end1 = (x_pos + x_offset+ 1) * tile_size
            start2 = (y_pos + y_offset) * tile_size
            end2 = (y_pos + y_offset + 1) * tile_size

            i0 = x_offset * BINS_PER_TILE
            i1 = i0 + BINS_PER_TILE + 1
            j0 = y_offset * BINS_PER_TILE
            j1 = j0 + BINS_PER_TILE + 1

            #print("resolution:", resolution)
            #print("tile_size", tile_size)
            #print("x_pos:", x_pos, "x_offset", x_offset)
            #print("start1", start1, 'end1', end1)
            #print("start2", start2, 'end2', end2)

            df = data[data['genome_start1'] >= start1]
            df = df[df['genome_start1'] < end1]

            df = df[df['genome_start2'] >= start2]
            df = df[df['genome_start2'] < end2]

            binsize = resolution

            j = ((df['genome_start1'].values - start1) // binsize).astype(int)
            i = ((df['genome_start2'].values - start2) // binsize).astype(int)

            if 'balanced' in df:
                v = np.nan_to_num(df['balanced'].values)
            else:
                v = np.nan_to_num(df['count'].values)

            out = np.zeros((256, 256), dtype=np.float32)
            out[i, j] = v

            if bins1 is not None and bins2 is not None:
                sub_bins1 = bins1[bins1['genome_start'] >= start1]
                sub_bins2 = bins2[bins2['genome_start'] >= start2]

                sub_bins1 = sub_bins1[sub_bins1['genome_start'] < end1]
                sub_bins2 = sub_bins2[sub_bins2['genome_start'] < end2]

                # print("sub_bins1:", sub_bins1)

                nan_bins1 = sub_bins1[np.isnan(sub_bins1['weight'])]
                nan_bins2 = sub_bins2[np.isnan(sub_bins2['weight'])]

                bi = ((nan_bins1['genome_start'].values - start1) // binsize).astype(int)
                bj = ((nan_bins2['genome_start'].values - start2) // binsize).astype(int)

                bend1 = ((np.array(range(total_length, int(end1), int(resolution))) - start1) // binsize).astype(int)
                bend2 = ((np.array(range(total_length, int(end2), int(resolution))) - start2) // binsize).astype(int)

                bend1 = bend1[bend1 >= 0]
                bend2 = bend2[bend2 >= 0]

                out[:, bi] = np.nan
                out[bj,:] = np.nan

                out[:, bend1] = np.nan
                out[bend2, :] = np.nan

            #print('sum(isnan1)', isnan1-1)
            #print('out.ravel()', sum(np.isnan(out.ravel())), len(out.ravel()))
            data_by_tilepos[(x_pos + x_offset, y_pos + y_offset)] = out.ravel()

    return data_by_tilepos

def bin_tiles_by_zoom_level_and_transform(tile_ids):
    '''
    Place these tiles into separate lists according to their
    zoom level and transform type

    Parameters
    ----------
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0.1) identifying the tiles
        to be retrieved

    Returns
    -------
    tile_lists: {(zoomLevel, transformType): [tile_id, tile_id]}
        A dictionary of tile ids
    '''
    tile_id_lists = col.defaultdict(set)

    for tile_id in tile_ids:
        tile_id_parts = tile_id.split('.')
        tile_position = list(map(int, tile_id_parts[1:4]))
        zoom_level = tile_position[0]

        transform_method = get_transform_type(tile_id)


        tile_id_lists[(zoom_level, transform_method)].add(tile_id)

    return tile_id_lists

def get_transform_type(tile_id):
    '''
    Get the transform type specified in the tile id.
    Parameters
    ----------
    cooler_tile_id: str
        A tile id for a 2D tile (cooler)
    Returns
    -------
    transform_type: str
        The transform type requested for this tile
    '''
    tile_id_parts = tile_id.split('.')

    if len(tile_id_parts) > 4:
        transform_method = tile_id_parts[4]
    else:
        transform_method = 'default'

    return transform_method


def get_available_transforms(cooler):
    '''
    Get the available resolutions from a single cooler file.
    Parameters
    ----------
    cooler: h5py File
        A cooler file containing binned 2D data
    Returns
    -------
    transforms: dict
        A list of transforms available for this dataset
    '''
    transforms = set()

    f_for_zoom = cooler['bins']

    if 'weight' in f_for_zoom:
        transforms.add('weight')
    if 'KR' in f_for_zoom:
        transforms.add('KR')
    if 'VC' in f_for_zoom:
        transforms.add('VC')
    if 'VC_SQRT' in f_for_zoom:
        transforms.add('VC_SQRT')

    return transforms

def make_mats(filepath):
    '''
    Create the file handle and tileset info for a cooler
    tileset
    '''
    f = h5py.File(filepath, 'r')

    if 'resolutions' in f:
        # this file contains raw resolutions so it'll return a different
        # sort of tileset info
        info = {"resolutions": tuple(sorted(map(int, list(f['resolutions'].keys())))) }
        mats[filepath] = [f, info]

        # see which transforms are available, a transform has to be
        # available at every available resolution in order for it to
        # be provided as an option
        available_transforms_per_resolution = {}

        for resolution in info['resolutions']:
            available_transforms_per_resolution[resolution] = get_available_transforms(f['resolutions'][str(resolution)])

        all_available_transforms = set.intersection(*available_transforms_per_resolution.values())

        info['transforms'] = [transform_descriptions[t] for t in all_available_transforms]

        # get the genome size
        resolution = list(f['resolutions'].keys())[0]
        genome_length = int(sum(f['resolutions'][resolution]['chroms']['length']))

        info['max_pos'] = [genome_length, genome_length]
        info['min_pos'] = [1,1]

        c = cooler.Cooler(f['resolutions'][resolution])
        info['chromsizes'] =  [[x[0], int(x[1])] for x in c.chromsizes.iteritems()]
        return (f, info)

    info = get_info(filepath)

    c = cooler.Cooler(f['0'])

    info['chromsizes'] =  [[x[0], int(x[1])] for x in c.chromsizes.iteritems()]
    info["min_pos"] = [int(m) for m in info["min_pos"]]
    info["max_pos"] = [int(m) for m in info["max_pos"]]
    info["max_zoom"] = int(info["max_zoom"])
    info["max_width"] = int(info["max_width"])

    if 'symmetric' in f['0'].attrs and not f['0'].attrs['symmetric']:
        info['mirror_tiles'] = 'false'

    if "transforms" in info:
        info["transforms"] = list(info["transforms"])

    mats[filepath] = [f, info]
    return (f, info)


def tileset_info(filepath):
    '''
    Get the tileset info for a cooler file

    Parameters:
    -----------

    filepath: str
        The location of the cooler file
    '''
    if filepath in mats:
        return mats[filepath][1]
    else:
        (f, info) = make_mats(filepath)

        return info

def add_transform_type(tile_id):
    '''
    Add a transform type to a cooler tile id if it's not already
    present.

    Parameters
    ----------
    tile_id: str
        A tile id (e.g. xyz.0.1.0)

    Returns
    -------
    new_tile_id: str
        A formatted tile id, potentially with an added transform_type
    '''
    tile_id_parts = tile_id.split('.')
    tileset_uuid = tile_id_parts[0]
    tile_position = tile_id_parts[1:4]

    transform_type = get_transform_type(tile_id)
    new_tile_id = ".".join([tileset_uuid] + tile_position + [transform_type])
    return new_tile_id

def tiles(filepath, tile_ids):
    '''
    '''
    transform_id_to_original_id = {}

    new_tile_ids = []

    for tile_id in tile_ids:
        new_tile_id = add_transform_type(tile_id)
        transform_id_to_original_id[new_tile_id] = tile_id
        new_tile_ids += [new_tile_id]

    generated_tiles = generate_tiles(filepath, new_tile_ids)

    tiles_to_return = []
    for tile_id, tile_value in generated_tiles:
        if tile_id in transform_id_to_original_id:
            original_tile_id = transform_id_to_original_id[tile_id]

            tiles_to_return += [(original_tile_id, tile_value)]

    return tiles_to_return


def generate_tiles(filepath, tile_ids):
    '''
    Generate tiles from a cooler file.
    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0.1) identifying the tiles
        to be retrieved
    Returns
    -------
    generated_tiles: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    '''
    BINS_PER_TILE = 256

    if filepath not in mats:
        # check if this tileset is open
        make_mats(filepath)

    tileset_file_and_info = mats[filepath]

    tile_ids_by_zoom_and_transform = bin_tiles_by_zoom_level_and_transform(tile_ids).values()
    partitioned_tile_ids = list(it.chain(*[hgut.partition_by_adjacent_tiles(t)
        for t in tile_ids_by_zoom_and_transform]))

    generated_tiles = []

    for tile_group in partitioned_tile_ids:
        zoom_level = int(tile_group[0].split('.')[1])
        tileset_id = tile_group[0].split('.')[0]
        transform_type = get_transform_type(tile_group[0])
        tileset_info = tileset_file_and_info[1]
        tileset_file = tileset_file_and_info[0]

        if 'resolutions' in tileset_info:
            sorted_resolutions = sorted([int(r) for r in tileset_info['resolutions']], reverse=True)
            if zoom_level > len(sorted_resolutions):
                # this tile has too high of a zoom level specified
                continue

            resolution = sorted_resolutions[zoom_level]
            hdf_for_resolution = tileset_file['resolutions'][str(resolution)]
        else:
            if zoom_level > tileset_info['max_zoom']:
                # this tile has too high of a zoom level specified
                continue
            hdf_for_resolution = tileset_file[str(zoom_level)]
            resolution = (tileset_info['max_width'] / 2**zoom_level) / BINS_PER_TILE

        tile_positions = [[int(x) for x in t.split('.')[2:4]] for t in tile_group]

        # filter for tiles that are in bounds for this zoom level
        tile_positions = list(filter(lambda x: x[0] < tileset_info['max_pos'][0]+1, tile_positions))
        tile_positions = list(filter(lambda x: x[1] < tileset_info['max_pos'][1]+1, tile_positions))

        if len(tile_positions) == 0:
            # no in bounds tiles
            continue

        minx = min([t[0] for t in tile_positions])
        maxx = max([t[0] for t in tile_positions])

        miny = min([t[1] for t in tile_positions])
        maxy = max([t[1] for t in tile_positions])

        tile_data_by_position = make_tiles(hdf_for_resolution,
                resolution,
                minx, miny,
                transform_type,
                maxx-minx+1, maxy-miny+1)

        tiles = [(".".join(map(str, [tileset_id] + [zoom_level] + list(position) + [transform_type])),
            hgfo.format_dense_tile(tile_data))
                for (position, tile_data) in tile_data_by_position.items()]


        generated_tiles += tiles

    return generated_tiles
