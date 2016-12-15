from __future__ import print_function

import json
import math
import numpy as np
import sys
import time
import clodius.fast as cf

def aggregate(in_array, num_to_agg):
    return cf.aggregate(in_array.astype(np.float32), num_to_agg)

def load_entries_from_file(sc, filename, column_names=None, delimiter=None, 
        elasticsearch_path=None):
    '''
    Load a dataset from file.

    :parma sc: SparkContext (or FakeSparkContext)
    :param filename: The filename for the file (either JSON or tsv) containing
        the data
    :param column_names: If the passed file is a tsv, then we may want to pass
        in column_names. If column_names is None then we assume that the file 
        contains column_names. column_names should be an array.
    :return: An array of dictionaries containing the data.
    '''

    def add_column_names(x):
        return dict(zip(column_names, x))

    if delimiter is not None:
        entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split(delimiter))))
    else:
        entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split())))
    
    return entries

def expand_range(x, from_col, to_col, range_except_0 = None):
    '''
    Copy a row multiple times if two columns indicate that it
    represents a range of values. The most common example would 
    be a bed file.

    chr1 1000 2000 4.6

    Means that the nucleotides between 1000 and 2000 on chromosome 1
    have a value of 4.6
    '''
    new_xs = []
    if range_except_0 is not None:
        if x[range_except_0] == '0':
            return []

    for i in range(int(x[from_col]), int(x[to_col])):
        new_x = x.copy()
        new_x[from_col] = i
        new_x[to_col] = i+1
        new_xs += [new_x]
    return new_xs

def summarize_data(max_entries):
    '''
    Summarize the data into a maximum of max_entries.

    :return: A function that can be called on a dataset to 
    condense it into a maximum of max_entries entries.
    '''
    def condense(data):
        random.shuffle(data)
        return data[:max_entries]

    return condense

def data_bounds(entries, num_dims):
    '''
    Get the minimum and maximum values for a data set.

    :param entries: A list of dictionaries representing the data
    :param num_dims: The number of dimensions in the data set
    :return: (mins, maxs)
    '''

    mins = [entries.map(lambda x: x['pos'][i]).reduce(reduce_min) for i in range(num_dims)]
    maxs = [entries.map(lambda x: x['pos'][i]).reduce(reduce_max) for i in range(num_dims)]

    return (mins, maxs)

def add_pos(dim_names, add_uuid=False):
    def add_pos_func(entry):
        new_dict = entry
        new_dict['pos'] = [float(entry[dn]) for dn in dim_names]

        if add_uuid:
            new_dict['uuid'] = shortuuid.uuid()

    return add_pos_func

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def make_tiles_by_importance(sc,entries, dim_names, max_zoom, mins, maxs, importance_field=None,
        max_entries_per_tile=10, output_dir=None, gzip_output=False, add_uuid=False,
        reverse_importance=False, end_dim_names=None, adapt_zoom=True):
    '''
    Create a set of tiles by restricting the maximum number of entries that
    can be shown on each tile. If there are too many entries that are assigned
    to a particular tile, the lower importance ones will be removed.

    If no importance is given, the entries will be selected randomly.

    :param sc: SparkContext
    :param entries: A list of dictionary containing the data which we wish to tile
    :param dim_names: The names of the dimensions along which we wish to break
                      up the tiles
    :param max_zoom: If it's None, then we will automatically calculate the maximum
                     zoom level such that all entries are shown.
    :param importance_field: The field which contains the importance of the entries.
    :return: A set of tiles
    '''
    if end_dim_names is None:
        end_dim_names = dim_names


    entries = entries.map(lambda x: merge_two_dicts(x, {'pos': [float(x[dn]) for dn in dim_names]}))
    entries = entries.map(lambda x: merge_two_dicts(x, {'end_pos': [float(x[dn]) for dn in end_dim_names]}))

    '''
    entry_ranges = entries.map(lambda x: ([x[importance_field]] + x['pos'] + x['end_pos'],
                                          [x[importance_field]] + x['pos'] + x['end_pos']))


    reduced_entry_ranges = entry_ranges.reduce(reduce_range)

    #mins = reduced_entry_ranges[0][1:1+len(dim_names)]
    #maxs = reduced_entry_ranges[1][1+len(dim_names):]
    '''

    max_width = max(map(lambda x: x[1] - x[0], zip(mins, maxs)))

    zoom_level = 0
    tile_entries = sc.parallelize([])

    if max_zoom is None:
        # hopefully it'll end up being smaller than that
        max_zoom = 1024

    if importance_field is None:
        importance_field = dim_names[0]

    # add zoom levels until we either reach the maximum zoom level or
    # have no tiles that have more entries than max_entries_per_tile
    while zoom_level <= max_zoom:
        tile_width = max_width / 2**zoom_level

        if tile_width == 0:
            tile_width = 1

        def place_in_tile(entry):
            tile_positions = []

            for (i,mind) in enumerate(mins):
                curr_pos = entry['pos'][i]
                dimension_tile_positions = [int((curr_pos - mind) / tile_width)]

                # if this feature spans multiple tiles, its end will be after the start
                # of the next tile
                next_tile_start_pos = mind + ((curr_pos - mind) // tile_width + 1) * tile_width

                while next_tile_start_pos < entry['end_pos'][i]:
                    # spans into the next tile
                    dimension_tile_positions += [int((next_tile_start_pos - mind) // tile_width)]
                    next_tile_start_pos += tile_width

                tile_positions += [dimension_tile_positions]

            # transpose the tile positions
            output_positions = [(tuple([zoom_level] + p), [entry]) for p in map(list, zip(*tile_positions))]

            return output_positions

        current_tile_entries = entries.flatMap(place_in_tile)
        current_max_entries_per_tile = max(current_tile_entries.countByKey().values())
        tile_entries = tile_entries.union(current_tile_entries)

        if adapt_zoom and current_max_entries_per_tile <= max_entries_per_tile:
            max_zoom = zoom_level
            break

        zoom_level += 1

    # all entries are broked up into ((tile_pos), [entry]) tuples
    # we just need to reduce the tiles so that no tile contains more than
    # max_entries_per_tile entries
    # (notice that [entry] is an array), this format will be important when
    # reducing to the most important values
    def reduce_values_by_importance(entry1, entry2):
        if reverse_importance:
            combined_entries = sorted(entry1 + entry2,
                    key=lambda x: -float(x[importance_field]))
        else:
            combined_entries = sorted(entry1 + entry2,
                    key=lambda x: float(x[importance_field]))
        return combined_entries[:max_entries_per_tile]

    reduced_tiles = tile_entries.reduceByKey(reduce_values_by_importance)

    tileset_info = {}
    tileset_info['max_importance'] = entries.map(lambda x: float(x[importance_field])).reduce(reduce_max)
    tileset_info['min_importance'] = entries.map(lambda x: float(x[importance_field])).reduce(reduce_min)

    tileset_info['min_pos'] = mins
    tileset_info['max_pos'] = maxs

    tileset_info['max_zoom'] = max_zoom
    tileset_info['max_width'] = max_width

    return {"tileset_info": tileset_info, "tiles": reduced_tiles}

def reduce_max(a,b):
    return max(a,b)

def reduce_min(a,b):
    return min(a,b)

def reduce_range(range_a, range_b):
    '''
    Get the range of two ranges, a and b.

    Example: a = [(1,3),(2,4)]
             b = [(0,5),(7,10)]
             ------------------
          result [(0,3),(7,10)]
    '''
    (mins_a, maxs_a) = range_a
    (mins_b, maxs_b) = range_b
    mins_c = [min(a,b) for (a,b) in zip(mins_a, mins_b)]
    maxs_c = [max(a,b) for (a,b) in zip(maxs_a, maxs_b)]

    return mins_c, maxs_c

def reduce_bins(bins_a, bins_b):
    for bin_pos in bins_b:
        bins_a[bin_pos] += bins_b[bin_pos]
    return bins_a

def reduce_sum(a,b):
    return a + b

def make_tiles_by_binning(sc, entries, dim_names, max_zoom, value_field='count', 
        importance_field='count', resolution=None,
        bins_per_dimension=1,
        num_histogram_bins=1000):
    '''
    Create tiles by calculating tile indeces.

    The first tile will encompass the entire data set, and each
    subsequent tile will contain a fraction of that data.

    :param dim_names: The names of the fields containing the positions of the data
    :param max_zoom: The maximum zoom level
    :param resolution: The resolution of the data
    :param aggregate_tile: Condense the entries in a given tile 
        (should operate on a single tile)
    '''
    max_data_in_sparse = bins_per_dimension ** len(dim_names) / 30.

    epsilon = 0.0000    # for calculating the max width so that all entries
                        # end up in the same top_level bucket

    # if the resolution is provided, we could go from the bottom up
    # and create zoom_widths that are multiples of the resolutions
    def consolidate_positions(entry):
        '''
        Place all of the dimensions in one array for this entry.
        '''
        new_entry = { 'pos': [float(entry[dn]) for dn in dim_names],
                      'value': float(entry[value_field]),
                      'importance': float(entry[importance_field]) }
        return new_entry

    def add_sparse_tile_metadata(tile_stuff):
        (tile_id, tile_entries_iterator) = tile_stuff
        z = tile_id[0]
        tile_width = max_width / 2 ** z
        bin_width = tile_width / bins_per_dimension

        # calculate where the tile values start along each dimension
        '''
        tile_start_pos = map(lambda (z,(m,x)): m + x * tile_width, 
                it.izip(it.cycle([tile_id[0]]), zip(mins, tile_id[1:])))

        tile_end_pos = map(lambda (z,(m,x)): m + (x+1) * tile_width, 
                it.izip(it.cycle([tile_id[0]]), zip(mins, tile_id[1:])))
        '''

        shown = []
        for (bin_pos, bin_val) in tile_entries_iterator:
            #pos = map(lambda(md, x): md + x * bin_width, zip(tile_start_pos, bin_pos))
            shown += [{'pos': bin_pos, 'value' : bin_val}]

        # caclulate where the tile values end along each dimension
        '''
        tile_data = {'shown': shown,
                     'zoom': tile_id[0],
                     'tile_start_pos': tile_start_pos,
                     'tile_end_pos': tile_end_pos}
        '''
        tile_data = shown

        return (tile_id, tile_data)

    # add the tile meta-data
    def add_dense_tile_metadata(tile_stuff):
        '''
        Add the tile's start and end data positions.
        '''
        (tile_id, tile_entries_iterator) = tile_stuff
        shown = []

        initial_values = [0.0] * (bins_per_dimension ** len(dim_names))

        for (bin_pos, val) in tile_entries_iterator:
            index = sum([bp * bins_per_dimension ** i for i,bp in enumerate(bin_pos)])
            initial_values[index] = val

        shown = initial_values

        # caclulate where the tile values end along each dimension
        tile_data = shown

        return (tile_id, tile_data)

    def add_tile_metadata(tile):
        if len(tile[1]) > max_data_in_sparse:
            return add_dense_tile_metadata(tile)
        else:
            return add_sparse_tile_metadata(tile)

    def dense_range(tile_value):
        return (min(tile_value), max(tile_value))

    def sparse_range(tile_value):
        values = [x['value'] for x in tile_value]

        return (0, max(values))

    def tile_pos_to_string(tile):
        (key, tile_value) = tile
        if len(tile_value) > max_data_in_sparse:
            (min_value, max_value) = dense_range(tile_value)
            output_str = {'dense': tile_value}
        else:
            (min_value, max_value) = sparse_range(tile_value)
            output_str = {'sparse': tile_value}

        output_str['min_value'] = min_value
        output_str['max_value'] = max_value

        return (key, output_str)

    entries = entries.map(consolidate_positions)
    tiled_entries = entries.map(lambda x: (0, x))
    all_tiles = sc.parallelize([])

    tileset_info = {}

    entry_ranges = entries.map(lambda x: ([x['value'], x['importance']] + x['pos'],
                                         ([x['value'], x['importance']] + x['pos'])))
    reduced_entry_ranges = entry_ranges.reduce(reduce_range)

    tileset_info['max_value'] = reduced_entry_ranges[1][0]
    tileset_info['min_value'] = reduced_entry_ranges[0][0]

    tileset_info['max_importance'] = reduced_entry_ranges[1][1]
    tileset_info['min_importance'] = reduced_entry_ranges[0][1]

    mins = reduced_entry_ranges[0][2:]
    maxs = reduced_entry_ranges[1][2:]

    value_histogram = []
    bin_size = (tileset_info['max_value'] - tileset_info['min_value']) / num_histogram_bins

    if bin_size == 0:
        bin_size = 1   # min_value == max_value

    histogram_counts = entries.map(lambda x: (int((x['value'] - tileset_info['min_value']) / bin_size), 1)).countByKey().items()
    histogram = {"min_value": tileset_info['min_value'],
                 "max_value": tileset_info['max_value'],
                 "counts": histogram_counts}

    # O(n) get the maximum and minimum bounds of the data set
    #(mins, maxs) = data_bounds(entries, len(dim_names))
    max_width = max(map(lambda x: x[1] - x[0] + epsilon, zip(mins, maxs)))

    if resolution is not None:
        # r * 2 ** n-1 < max_width < r * 2 ** n
        # we need a max width that is a multiple of the resolution, the bin size
        # and a power of 2. 
        if bins_per_dimension is None:
            bins_per_dimension = 1

        bins_to_display_at_max_resolution = max_width / resolution / bins_per_dimension
        max_max_zoom = math.ceil(math.log(bins_to_display_at_max_resolution) / math.log(2.))

        max_width = resolution * bins_per_dimension * 2 ** max_max_zoom

        if max_max_zoom < 0:
            max_max_zoom = 0

        if max_max_zoom < max_zoom:
            max_zoom = int(max_max_zoom)

    # get all the different zoom levels
    zoom_levels = range(max_zoom+1)

    zoom_width = max_width / 2 ** max_zoom
    bin_width = zoom_width / bins_per_dimension

    tileset_info['min_pos'] = mins
    tileset_info['max_pos'] = maxs

    tileset_info['max_zoom'] = max_zoom
    tileset_info['max_width'] = max_width

    tileset_info['data_granularity'] = resolution
    tileset_info['bins_per_dimension'] = bins_per_dimension

    def place_positions_at_origin(entry):
        #new_pos = map(lambda (x, mx): x - mx, zip(entry['pos'], mins))
        new_pos = [x - mx for (x,mx) in zip(entry['pos'], mins)]
        return (new_pos, entry['value'])

    # add a bogus tile_id for downstream processing
    bin_entries = entries.map(place_positions_at_origin)
    total_bins = 0
    total_tiles = 0
    total_start_time = time.time()

    for zoom_level in range(0, max_zoom+1)[::-1]:
        tile_width = max_width / 2 ** zoom_level
        bin_width = tile_width / bins_per_dimension
        start_time = time.time()

        def place_in_bin(pos_value):
            (prev_pos, value) = pos_value
            new_bin_pos = tuple([int(int(x / bin_width) * bin_width) for x in prev_pos])

            return (new_bin_pos, value)

        def place_in_tile(bin_pos_value):
            (bin_pos, value) = bin_pos_value

            # we have a bin position and we need to place it in a tile
            tile_pos = [int(x / tile_width) for x in bin_pos]
            tile_mins = [x * tile_width for x in tile_pos]
            bin_in_tile = [int((bin_pos[i] - mind) / bin_width) for (i,mind) in enumerate(tile_mins)]

            return (tuple([zoom_level] + tile_pos), [(bin_in_tile, value)])


        bin_entries = bin_entries.map(place_in_bin).reduceByKey(reduce_sum)
        '''
        bin_count = bin_entries.count()
        total_bins += bin_count
        '''

        tile_entries = bin_entries.map(place_in_tile)
        tile_entries = tile_entries.reduceByKey(reduce_sum)
        '''
        tile_count = tile_entries.count()
        total_tiles += tile_count
        '''

        '''

        def place_in_tile(entry):
            tile_pos = tuple( [zoom_level] + 
                    map(lambda (i, mind): int((entry['pos'][i] - mind) / tile_width),
                           enumerate(mins)))

            tile_mins = map(lambda (z,(m,x)): m + x * (max_width / 2 ** z), 
                    it.izip(it.cycle([tile_pos[0]]), zip(mins, tile_pos[1:])))

            bin_pos = map(lambda (i, mind): int((entry['pos'][i] - mind) / bin_width),
                          enumerate(tile_mins))
            bin_dict = col.defaultdict(float)
            bin_dict[tuple(bin_pos)] = entry[value_field]
            return ((tile_pos), bin_dict)
        
        tile_entries = entries.map(place_in_tile).reduceByKey(reduce_bins)
        '''
        tiles_with_meta = tile_entries.map(add_tile_metadata)
        tiles_with_meta_string = tiles_with_meta.map(tile_pos_to_string)
        all_tiles = all_tiles.union(tiles_with_meta_string)



        end_time = time.time()


    total_end_time = time.time()
    #total_entries = entries.count()

    return {"tileset_info": tileset_info, "tiles": all_tiles, "histogram": histogram}
