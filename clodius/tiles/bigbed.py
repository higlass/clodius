import bbi
import functools as ft
import logging
import numpy as np
import pandas as pd
import re
import sys
import random

from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 4
TILE_SIZE = 1024
MIN_ELEMENTS = 1
MAX_ELEMENTS = 50

logger = logging.getLogger(__name__)

range_modes = {}
range_modes['significant'] = {'name': 'Significant', 'value': 'significant'}


def get_quadtree_depth(chromsizes):
    tile_size_bp = TILE_SIZE
    min_tile_cover = np.ceil(sum(chromsizes) / tile_size_bp)
    return int(np.ceil(np.log2(min_tile_cover)))


def get_zoom_resolutions(chromsizes):
    return [2**x for x in range(get_quadtree_depth(chromsizes) + 1)][::-1]


def natsort_key(s, _NS_REGEX=re.compile(r'(\d+)', re.U)):
    return tuple(
        [int(x) if x.isdigit() else x for x in _NS_REGEX.split(s) if x]
    )


def natcmp(x, y):
    if x.find('_') >= 0:
        x_parts = x.split('_')
        if y.find('_') >= 0:
            # chr_1 vs chr_2
            y_parts = y.split('_')

            return natcmp(x_parts[1], y_parts[1])
        else:
            # chr_1 vs chr1
            # chr1 comes first
            return 1
    if y.find('_') >= 0:
        # chr1 vs chr_1
        # y comes second
        return -1

    _NS_REGEX = re.compile(r'(\d+)', re.U)
    x_parts = tuple(
        [int(a) if a.isdigit() else a for a in _NS_REGEX.split(x) if a]
    )
    y_parts = tuple(
        [int(a) if a.isdigit() else a for a in _NS_REGEX.split(y) if a]
    )

    # order of these parameters is purposefully reverse how they should be
    # ordered
    for key in ['m', 'y', 'x']:
        if key in y.lower():
            return -1
        if key in x.lower():
            return 1

    try:
        if x_parts < y_parts:
            return -1
        elif y_parts > x_parts:
            return 1
        else:
            return 0
    except TypeError:
        return 1


def natsorted(iterable):
    return sorted(iterable, key=ft.cmp_to_key(natcmp))


def get_chromsizes(bbpath):
    """
    TODO: replace this with negspy

    Also, return NaNs from any missing chromosomes in bbi.fetch

    """
    chromsizes = bbi.chromsizes(bbpath)
    chromosomes = natsorted(chromsizes.keys())
    chrom_series = pd.Series(chromsizes)[chromosomes]
    return chrom_series


def abs2genomic(chromsizes, start_pos, end_pos):
    abs_chrom_offsets = np.r_[0, np.cumsum(chromsizes.values)]
    cid_lo, cid_hi = np.searchsorted(abs_chrom_offsets,
                                     [start_pos, end_pos],
                                     side='right') - 1
    rel_pos_lo = start_pos - abs_chrom_offsets[cid_lo]
    rel_pos_hi = end_pos - abs_chrom_offsets[cid_hi]
    start = rel_pos_lo
    for cid in range(cid_lo, cid_hi):
        yield cid, start, chromsizes[cid]
        start = 0
    yield cid_hi, start, rel_pos_hi


def tileset_info(bbpath, chromsizes=None):
    '''
    Get the tileset info for a bigWig file

    Parameters
    ----------
    bbpath: string
        The path to the bigbed file from which to retrieve data
    chromsizes: [[chrom, size],...]
        A list of chromosome sizes associated with this tileset.
        Typically passed in to specify in what order data from
        the bigbed should be returned.

    Returns
    -------
    tileset_info: {
                   'min_pos': [],
                   'max_pos': [],
                   'tile_size': 1024,
                   'max_zoom': 7
                  }
    '''
    TILE_SIZE = 1024

    if chromsizes is None:
        chromsizes = get_chromsizes(bbpath)
        chromsizes_list = []

        for chrom, size in chromsizes.iteritems():
            chromsizes_list += [[chrom, int(size)]]
    else:
        chromsizes_list = chromsizes

    min_tile_cover = np.ceil(
        sum([int(c[1]) for c in chromsizes_list]) / TILE_SIZE
    )
    max_zoom = int(np.ceil(np.log2(min_tile_cover)))

    tileset_info = {
        'min_pos': [0],
        'max_pos': [TILE_SIZE * 2 ** max_zoom],
        'max_width': TILE_SIZE * 2 ** max_zoom,
        'tile_size': TILE_SIZE,
        'max_zoom': max_zoom,
        'chromsizes': chromsizes_list,
        'range_modes': range_modes
    }
    return tileset_info


def fetch_data(a):
    (
        bbpath,
        binsize,
        chromsizes,
        range_mode,
        min_elements,
        max_elements,
        cid,
        start,
        end
    ) = a
    n_bins = int(np.ceil((end - start) / binsize))

    try:
        chrom = chromsizes.index[cid]
        clen = chromsizes.values[cid]

        kwargs = {"bins": n_bins, "missing": np.nan}
        
        fetch_factory = ft.partial(bbi.fetch_intervals, bbpath, chrom, start, end)

        if range_mode == 'significant':
            intervals, intervals2 = fetch_factory(), fetch_factory()

        else:
            intervals, intervals2 = fetch_factory(), fetch_factory()

    except IndexError:
        # beyond the range of the available chromosomes
        # probably means we've requested a range of absolute
        # coordinates that stretch beyond the end of the genome
        intervals, intervals2 = None, None
        
    except KeyError:
        # probably requested a chromosome that doesn't exist (e.g. chrM)
        intervals, intervals2 = None, None
        
    offset = 0
    offsetIdx = 0
    chrOffsets = {}
    for chrSize in chromsizes:
        chrOffsets[chromsizes.index[offsetIdx]] = offset
        offset += chrSize
        offsetIdx += 1
        
    final_intervals = []
    intervals_length = 0
    scores = []
    
    if not intervals:
        return final_intervals
    
    for interval in intervals:
        scores.append(int(interval[4]))
        intervals_length += 1
    
    # generate beddb-like elements for parsing by the higlass plugin
    if intervals_length >= min_elements and intervals_length <= max_elements:
        for interval in intervals2:
            final_intervals.append({'chrOffset': chrOffsets[chrom], 'importance': int(interval[4]), 'fields': interval})
            
    elif intervals_length > max_elements:
        thresholded_intervals = []
        desired_perc = max_elements / intervals_length
        thresholded_score = int(np.quantile(scores, 1-desired_perc))
        for interval in intervals2:
            if int(interval[4]) >= thresholded_score:
                thresholded_intervals.append({'chrOffset': chrOffsets[chrom], 'importance': int(interval[4]), 'fields': interval})
        thresholded_intervals_length = len(thresholded_intervals)
        if thresholded_intervals_length > max_elements:
            indices = random.sample(range(thresholded_intervals_length), max_elements)
            final_intervals = [thresholded_intervals[i] for i in sorted(indices)]
 
    return final_intervals


def get_bigbed_tile(
    bbpath,
    zoom_level,
    start_pos,
    end_pos,
    chromsizes=None,
    range_mode=None,
    min_elements=None,
    max_elements=None
):
    if chromsizes is None:
        chromsizes = get_chromsizes(bbpath)
        
    if min_elements is None:
        min_elements = MIN_ELEMENTS
    if max_elements is None:
        max_elements = MAX_ELEMENTS

    resolutions = get_zoom_resolutions(chromsizes)
    binsize = resolutions[zoom_level]

    cids_starts_ends = list(abs2genomic(chromsizes, start_pos, end_pos))
    
    with ThreadPoolExecutor(max_workers=16) as e:
        arrays = list(
            e.map(
                fetch_data, [
                    tuple([
                        bbpath,
                        binsize,
                        chromsizes,
                        range_mode,
                        min_elements,
                        max_elements
                    ] + list(c)) for c in cids_starts_ends
                ]
            )
        )
        
    return [x for x in arrays if x != []]


def tiles(bbpath, tile_ids, chromsizes_map={}, chromsizes=None):
    '''
    Generate tiles from a bigbed file.

    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved
    chromsizes_map: {uid: []}
        A set of chromsizes listings corresponding to the parameters of the
        tile_ids. To be used if a chromsizes id is passed in with the tile id
        with the `|cos:id` tag in the tile id
    chromsizes: [[chrom, size],...]
        A 2d array containing chromosome names and sizes. Overrides the
        chromsizes in chromsizes_map

    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    '''
    TILE_SIZE = 1024
    min_elements = -1
    max_elements = -1
    
    generated_tiles = []
    for tile_id in tile_ids:
        tile_option_parts = tile_id.split('|')[1:]
        tile_no_options = tile_id.split('|')[0]
        tile_id_parts = tile_no_options.split('.')
        tile_position = list(map(int, tile_id_parts[1:3]))
        return_value = tile_id_parts[3] if len(tile_id_parts) > 3 else 'mean'

        range_mode = return_value if return_value in range_modes else None

        tile_options = dict([o.split(':') for o in tile_option_parts])

        if 'min' in tile_options:
            min_elements = int(tile_options['min'])
        if 'max' in tile_options:
            max_elements = int(tile_options['max'])
            
        if min_elements > max_elements:
            temp_min_elements = min_elements
            min_elements = max_elements
            max_elements = temp_min_elements 
        elif min_elements == max_elements:
            min_elements = max_elements
            max_elements = min_elements + 1
            
        if max_elements <= 0:
            min_elements = 1
            max_elements = 10

        if chromsizes:
            chromnames = [c[0] for c in chromsizes]
            chromlengths = [int(c[1]) for c in chromsizes]
            chromsizes_to_use = pd.Series(chromlengths, index=chromnames)
        else:
            chromsizes_id = None
            if 'cos' in tile_options:
                chromsizes_id = tile_options['cos']
            if chromsizes_id in chromsizes_map:
                chromsizes_to_use = chromsizes_map[chromsizes_id]
            else:
                chromsizes_to_use = None

        zoom_level = tile_position[0]
        tile_pos = tile_position[1]

        # this doesn't combine multiple consequetive ids, which
        # would speed things up
        if chromsizes_to_use is None:
            chromsizes_to_use = get_chromsizes(bbpath)

        max_depth = get_quadtree_depth(chromsizes_to_use)
        tile_size = TILE_SIZE * 2 ** (max_depth - zoom_level)
        start_pos = tile_pos * tile_size
        end_pos = start_pos + tile_size
    
        tile_value = get_bigbed_tile(
            bbpath,
            zoom_level,
            start_pos,
            end_pos,
            chromsizes_to_use,
            range_mode=range_mode,
            min_elements=min_elements,
            max_elements=max_elements
        )

        generated_tiles += [(tile_id, tile_value)]
    
    return generated_tiles


def chromsizes(filename):
    '''
    Get a list of chromosome sizes from this [presumably] bigwig
    file.

    Parameters:
    -----------
    filename: string
        The filename of the bigwig file

    Returns
    -------
    chromsizes: [(name:string, size:int), ...]
        An ordered list of chromosome names and sizes
    '''
    try:
        chrom_series = get_chromsizes(filename)
        data = []
        for chrom, size in chrom_series.iteritems():
            data.append([chrom, size])
        return data
    except Exception as ex:
        logger.error(ex)
        raise Exception(
            'Error loading chromsizes from bigwig file: {}'.format(ex)
        )
