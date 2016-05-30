#!/usr/bin/python

from time import gmtime, strftime
import argparse
import csv
import collections as col
import fpark
import gzip
import itertools as it
import json
import math
import os
import os.path as op
import random
import sys
import time

def expand_range(x, from_col, to_col):
    new_xs = []
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

def save_tile_template(output_dir, gzip_output, output_format='sparse'):
    def save_tile(tile):
        key = tile[0]
        tile_value = tile[1]

        outpath = op.join(output_dir, '/'.join(map(str, key)) + '.json')
        outdir = op.dirname(outpath)

        if not op.exists(outdir):
            try:
                os.makedirs(outdir)
            except OSError as oe:
                # somebody probably made the directory in between when we
                # checked if it exists and when we're making it
                print >>sys.stderr, "Error making directories:", oe

        if gzip_output:
            with gzip.open(outpath + ".gz", 'w') as f:
                f.write(tile_value)
        else:
            with open(outpath, 'w') as f:
                f.write(json.dumps(tile_value))

    def blah(x):
        return str(x)

    return save_tile
    #return blah

def load_entries_from_file(filename, column_names=None, use_spark=False, delimiter='\t', 
        elasticsearch_path=None):
    '''
    Load a dataset from file.

    :param filename: The filename for the file (either JSON or tsv) containing
        the data
    :param column_names: If the passed file is a tsv, then we may want to pass
        in column_names. If column_names is None then we assume that the file 
        contains column_names. column_names should be an array.
    :return: An array of dictionaries containing the data.
    '''
    sys.stderr.write("Loading entries...")
    sys.stderr.flush()

    def add_column_names(x):
        return dict(zip(column_names, x))

    global sc
    if use_spark:
        from pyspark import SparkContext
        sc = SparkContext()

        if delimiter is not None:
            entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split(delimiter))))
        else:
            entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split())))
        return entries
    else:
        sys.stderr.write("setting sc:")
        sc = fpark.FakeSparkContext
        if delimiter is not None:
            entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split(delimiter))))
        else:
            entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split())))
        sys.stderr.write(" done\n")
        return entries

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
        new_dict['pos'] = map(lambda dn: float(entry[dn]), dim_names)

        if add_uuid:
            new_dict['uuid'] = shortuuid.uuid()

    return add_pos_func

def make_tiles_by_importance(entries, dim_names, max_zoom, importance_field=None,
        max_entries_per_tile=10, output_dir=None, gzip_output=False, add_uuid=False,
        reverse_importance=False):
    '''
    Create a set of tiles by restricting the maximum number of entries that
    can be shown on each tile. If there are too many entries that are assigned
    to a particular tile, the lower importance ones will be removed.

    :param entries: A list of dictionary containing the data which we wish to tile
    :param dim_names: The names of the dimensions along which we wish to break
                      up the tiles
    :param max_zoom: If it's None, then we will automatically calculate the maximum
                     zoom level such that all entries are shown.
    :param importance_field: The field which contains the importance of the entries.
    :return: A set of tiles
    '''

    entries.map(add_pos(dim_names, add_uuid))

    (mins, maxs) = data_bounds(entries, len(dim_names))
    max_width = max(map(lambda x: x[1] - x[0], zip(mins, maxs)))

    zoom_level = 0
    tile_entries = sc.parallelize([])

    if max_zoom is None:
        # hopefully it'll end up being smaller than that
        max_zoom = sys.maxint

    # add zoom levels until we either reach the maximum zoom level or
    # have no tiles that have more entries than max_entries_per_tile
    while zoom_level < max_zoom:
        zoom_width = max_width / 2**zoom_level

        def place_in_tile(entry):
            tile_pos = tuple( [zoom_level] + 
                    map(lambda (i, mind): int((entry['pos'][i] - mind) / zoom_width),
                           enumerate(mins)))

            return ((tile_pos), [entry])

        current_tile_entries = entries.map(place_in_tile)
        current_max_entries_per_tile = max(current_tile_entries.countByKey().values())
        tile_entries = tile_entries.union(current_tile_entries)

        if current_max_entries_per_tile <= max_entries_per_tile:
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

    if output_dir is not None:
        save_tile = save_tile_template(output_dir, gzip_output)
        reduced_tiles.foreach(save_tile)

    return {"tileset_info": tileset_info, "tiles": reduced_tiles}

def reduce_max(a,b):
    return max(a,b)

def reduce_min(a,b):
    return min(a,b)

def reduce_range((mins_a, maxs_a), (mins_b, maxs_b)):
    '''
    Get the range of two ranges, a and b.

    Example: a = [(1,3),(2,4)]
             b = [(0,5),(7,10)]
             ------------------
          result [(0,3),(7,10)]
    '''
    mins_c = map(min, zip(mins_a, mins_b))
    maxs_c = map(max, zip(maxs_a, maxs_b))

    return mins_c, maxs_c

def reduce_bins(bins_a, bins_b):
    for bin_pos in bins_b:
        bins_a[bin_pos] += bins_b[bin_pos]
    return bins_a

def reduce_sum(a,b):
    return a + b

def make_tiles_by_binning(entries, dim_names, max_zoom, value_field='count', 
        importance_field='count', resolution=None,
        aggregate_tile=lambda tile,dim_names: tile, 
        bins_per_dimension=None, output_dir=None,
        elasticsearch_nodes=None,
        elasticsearch_path=None,
        gzip_output=False, output_format='sparse',
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
    print "max_data_in_sparse", max_data_in_sparse

    epsilon = 0.0000    # for calculating the max width so that all entries
                        # end up in the same top_level bucket

    # if the resolution is provided, we could go from the bottom up
    # and create zoom_widths that are multiples of the resolutions
    def consolidate_positions(entry):
        '''
        Place all of the dimensions in one array for this entry.
        '''
        new_entry = {'pos': map(lambda dn: float(entry[dn]), dim_names),
                      value_field: float(entry[value_field]),
                      importance_field: float(entry[importance_field]) }
        return new_entry

    def add_sparse_tile_metadata((tile_id, tile_entries_iterator)):
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
            shown += [{'pos': bin_pos, value_field : bin_val}]

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
    def add_dense_tile_metadata((tile_id, tile_entries_iterator)):
        '''
        Add the tile's start and end data positions.
        '''
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
        values = map(lambda x: x['count'], tile_value)

        return (0, max(values))

    def tile_pos_to_string((key, tile_value)):
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

    tileset_info = {}

    entry_ranges = entries.map(lambda x: ([x[value_field], x[importance_field]] + x['pos'],
                                         ([x[value_field], x[importance_field]] + x['pos'])))
    reduced_entry_ranges = entry_ranges.reduce(reduce_range)

    tileset_info['max_value'] = reduced_entry_ranges[1][0]
    tileset_info['min_value'] = reduced_entry_ranges[0][0]

    tileset_info['max_importance'] = reduced_entry_ranges[1][1]
    tileset_info['min_importance'] = reduced_entry_ranges[0][1]

    mins = reduced_entry_ranges[0][2:]
    maxs = reduced_entry_ranges[1][2:]

    print "mins:", mins
    print "maxs:", maxs

    value_histogram = []
    bin_size = (tileset_info['max_value'] - tileset_info['min_value']) / num_histogram_bins

    if bin_size == 0:
        bin_size = 1   # min_value == max_value

    histogram_counts = entries.map(lambda x: (int((x[value_field] - tileset_info['min_value']) / bin_size), 1)).countByKey().items()
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

    print "max_zoom:", max_zoom

    # get all the different zoom levels
    zoom_levels = range(max_zoom+1)
    zoom_widths = map(lambda x: max_width / 2 ** x, zoom_levels)
    zoom_level_widths = zip(zoom_levels, zoom_widths)

    zoom_width = max_width / 2 ** max_zoom
    bin_width = zoom_width / bins_per_dimension

    tileset_info['min_pos'] = mins
    tileset_info['max_pos'] = maxs

    tileset_info['max_zoom'] = max_zoom
    tileset_info['max_width'] = max_width

    tileset_info['data_granularity'] = resolution
    tileset_info['bins_per_dimension'] = bins_per_dimension

    if elasticsearch_nodes is not None:
        es_url = op.join(elasticsearch_nodes, elasticsearch_path)
        import urllib2 as urllib
        import requests

        def save_tile_to_elasticsearch(partition):
            bulk_txt = ""
            put_url =  op.join(es_url, "_bulk")

            for val in partition:
                bulk_txt += json.dumps({"index": {"_id": val['tile_id']}}) + "\n"
                bulk_txt += json.dumps(val) + "\n"

                if len(bulk_txt) > 50000000:
                    try:
                        requests.post("http://" + put_url, data=bulk_txt)
                    except ConnectionError as ce:
                        print >>sys.stderr, "Error saving to elasticsearch:", ce
                    bulk_txt = ""

            #print "len(bulk_txt)", len(bulk_txt)
            requests.post("http://" + put_url, data=bulk_txt)

        tileset_info_rdd = sc.parallelize([{"tile_value": tileset_info, "tile_id": "tileset_info"}])
        tileset_info_rdd.foreachPartition(save_tile_to_elasticsearch)

        histogram_rdd = sc.parallelize([{"tile_value": histogram, "tile_id": "histogram"}])
        histogram_rdd.foreachPartition(save_tile_to_elasticsearch)
    else:
        with open(op.join(output_dir, 'tile_info.json'), 'w') as f:
            json.dump(tileset_info, f, indent=2)

        with open(op.join(output_dir, 'value_histogram.json'), 'w') as f:
            json.dump(histogram, f, indent=2)

    def place_positions_at_origin(entry):
        new_pos = map(lambda (x, mx): x - mx, zip(entry['pos'], mins))
        return (new_pos, entry[value_field])

    # add a bogus tile_id for downstream processing
    bin_entries = entries.map(place_positions_at_origin)
    total_bins = 0
    total_tiles = 0
    total_start_time = time.time()

    for zoom_level in range(0, max_zoom+1)[::-1]:
        tile_width = max_width / 2 ** zoom_level
        bin_width = tile_width / bins_per_dimension
        start_time = time.time()

        def place_in_bin((prev_pos, value)):
            new_bin_pos = tuple(map(lambda x: int(int(x / bin_width) * bin_width), prev_pos))

            return (new_bin_pos, value)

        def place_in_tile((bin_pos, value)):
            # we have a bin position and we need to place it in a tile
            tile_pos = tuple([zoom_level] + map(lambda x: int(x / tile_width), bin_pos))
            tile_mins = map(lambda x: x * tile_width, tile_pos[1:])
            bin_in_tile = map(lambda (i, mind): int((bin_pos[i] - mind) / bin_width),
                          enumerate(tile_mins))

            return ((tile_pos), [(bin_in_tile, value)])


        bin_entries = bin_entries.map(place_in_bin).reduceByKey(reduce_sum)
        '''
        bin_count = bin_entries.count()
        total_bins += bin_count
        '''

        tile_entries = bin_entries.map(place_in_tile).reduceByKey(reduce_sum)
        '''
        tile_count = tile_entries.count()
        total_tiles += tile_count
        '''


        #print "bin_entry:", zoom_level, bin_entries.count(), bin_entries.take(1)
        #print "tile_entry", zoom_level, tile_entries.count() 
        #print "zoom_level:", zoom_level, "count:", bin_entries.count()

        #print "tile_entry:", tile_entries.count(), tile_entries.take(1)
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

    #print tiles_with_meta_string.take(1)

        if elasticsearch_nodes is not None:

            tiles_as_jsons = tiles_with_meta_string.map(lambda x: {"tile_id": ".".join(map(str,x[0])), "tile_value": x[1]})
            tiles_as_jsons.foreachPartition(save_tile_to_elasticsearch)


        else:
            save_tile = save_tile_template(output_dir, gzip_output, output_format)
            tiles_with_meta_string.foreach(save_tile)

        end_time = time.time()
        #print "zoom_level:", zoom_level, "bin_entries:", bin_count, "tile_entries:", tile_count, "time:", int(end_time - start_time), "time_per_Mbin:", int(1000000 * (end_time - start_time) / bin_count)
        print "zoom_level:", zoom_level, "time:", int(end_time - start_time)


    total_entries = entries.count()
    total_end_time = time.time()
    #print "save time:", strftime("%Y-%m-%d %H:%M:%S", gmtime())
    #print "entries:", total_entries, "bins:", total_bins, "tiles:", total_tiles, "time:", int(total_end_time - total_start_time), 'time_per_Mbin:', int(1000000 * (total_end_time - total_start_time) / total_bins)
    print "total_time:", int(total_end_time - total_start_time)

    return {"tileset_info": tileset_info, "tiles": tiles_with_meta, "histogram": histogram}

def main():
    usage = """
    python make_tiles.py input_file

    Create tiles for all of the entries in the JSON file.
    """
    num_args= 1
    parser = argparse.ArgumentParser()

    #parser.add_argument('-o', '--options', dest='some_option', default='yo', help="Place holder for a real option", type='str')
    #parser.add_argument('-u', '--useless', dest='uselesss', default=False, action='store_true', help='Another useless option')
    parser.add_argument('input_file')
    parser.add_argument('-b', '--bins-per-dimension', 
                        help='The number of bins to divide the data into',
                        default=None,
                        type=int)
    parser.add_argument('--use-spark', default=False, action='store_true',
                        help='Use spark to distribute the workload')

    parser.add_argument('-r', '--resolution', 
                        help='The resolution of the data (applies only to matrix data)',
                        type=int)

    parser.add_argument('--importance', action='store_true', help='Create tiles by importance') 

    parser.add_argument('-i', '--importance-field', dest='importance_field', default='importance_field',
            help='The field in each JSON entry that indicates how important that entry is',
            type=str)
    parser.add_argument('-v', '--value', dest='value_field', default='count',
            help='The that has the value of each point. Used for aggregation and display')

    group = parser.add_mutually_exclusive_group()

    group.add_argument('-p', '--position', dest='position', default='position',
            help='Where this entry would be placed on the x axis',
            type=str)
    group.add_argument('-s', '--sort-by', 
            default=None,
            help='Sort by a field and use as the position') 

    parser.add_argument('-e', '--max-entries-per-tile', dest='max_entries_per_tile', default=100,
        help='The maximum number of entries that can be displayed on a single tile',
        type=int)
    parser.add_argument('-c', '--column-names', dest='column_names', default=None)
    parser.add_argument('-m', '--max-zoom', dest='max_zoom', default=None,
            help='The maximum zoom level', type=int)
    parser.add_argument('--min-pos', dest='min_pos', default=None,
            help='The minimum x position', type=float)
    parser.add_argument('--max-pos', dest='max_pos', default=None,
            help='The maximum x position', type=float)
    parser.add_argument('--min-value', 
            help='The field which will be used to determinethe minimum value for any data point', 
            default='min_y')
    parser.add_argument('--max-value', 
            help='The field which will be used to determine the maximum value for any data point', 
            default='max_y')
    parser.add_argument('--range',
            help="Use two columns to create a range (i.e. pos1,pos2",
            default=None)
    parser.add_argument('--gzip', help='Compress the output JSON files using gzip', 
            action='store_true')
    parser.add_argument('--output-format', 
            help='The format for the output matrix, can be either "dense" or "sparse"',
            default='sparse')
    parser.add_argument('--add-uuid',
            help='Add a uuid to each element',
            action='store_true',
            default=False)
    parser.add_argument('--reverse-importance',
            help='Reverse the ordering of the importance',
            action='store_true',
            default=False)

    output_group = parser.add_mutually_exclusive_group(required=True)

    output_group.add_argument('--elasticsearch-path',
            help='Send the output to an elasticsearch instance',
            default=None)
    output_group.add_argument('-o', '--output-dir', help='The directory to place the tiles',
            default=None)
    
    parser.add_argument('--delimiter',
            help="The delimiter separating the different columns in the input files",
            default=None)

    parser.add_argument('--elasticsearch-nodes', 
            help='Specify elasticsearch nodes to push the completions to',
            default=None)
    parser.add_argument('--elasticsearch-index',
            help="The index to place the results in",
            default='test')
    parser.add_argument('--elasticsearch-doctype',
            help="The type of document to index",
            default="autocomplete")


    args = parser.parse_args()

    if not args.importance:
        if args.output_format not in ['sparse', 'dense']:
            print >>sys.stderr, 'ERROR: The output format must be one of "dense" or "sparse"'

    dim_names = args.position.split(',')

    if args.column_names is not None:
        args.column_names = args.column_names.split(',')

    print "start time:", strftime("%Y-%m-%d %H:%M:%S", gmtime())
    entries = load_entries_from_file(args.input_file, args.column_names,
            args.use_spark, delimiter=args.delimiter,
            elasticsearch_path=args.elasticsearch_path)
    print "load entries time:", strftime("%Y-%m-%d %H:%M:%S", gmtime())

    if args.range is not None:
        # if a pair of columns specifies a range of values, then create multiple
        # entries for each value within that range (e.g. bed files)
        range_cols = args.range.split(',')
        entries = entries.flatMap(lambda x: expand_range(x, *range_cols))

    if args.importance:
        tileset = make_tiles_by_importance(entries, dim_names=args.position.split(','), 
                max_zoom=args.max_zoom, 
                importance_field=args.importance_field,
                output_dir=args.output_dir,
                gzip_output=args.gzip, add_uuid=args.add_uuid,
                reverse_importance=args.reverse_importance)
    else:
        tileset = make_tiles_by_binning(entries, args.position.split(','), 
                args.max_zoom, args.value_field, args.importance_field,
                bins_per_dimension=args.bins_per_dimension,
                resolution=args.resolution, output_dir=args.output_dir,
                gzip_output=args.gzip, output_format=args.output_format,
                elasticsearch_nodes=args.elasticsearch_nodes,
                elasticsearch_path=args.elasticsearch_path)

if __name__ == '__main__':
    main()


