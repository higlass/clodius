#!/usr/bin/python

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
#import shortuuid
import sys

sc = None

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

def load_entries_from_file(filename, column_names=None, use_spark=False):
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

    if use_spark:
        from pyspark import SparkContext
        sc = SparkContext(appName="Clodius")
        entries = sc.textFile(filename).map(lambda x: dict(zip(column_names, x.strip().split('\t'))))
        return entries
    else:
        with open(filename, 'r') as f:
            tsv_reader = csv.reader(f, delimiter='\t')

            entries = fpark.FakeSparkContext.parallelize(tsv_reader)
            entries = entries.map(add_column_names)
            
            sys.stderr.write(" done\n")
            sys.stderr.flush()

            return entries

def make_tiles_from_file(filename, options):
    '''
    Create tiles for a dataset stored in a file.

    The data set can be either in JSON or tsv format. We will first try loading
    it as JSON and if that fails, we will default to tsv.

    :param filename: The name of the file containing the data for which to
                     make tiles.

    :return: An array of tiles, each in json format.
    '''
    dim_names = options.position.split(',')

    if options.column_names is not None:
        options.column_names = options.column_names.split(',')

    entries = load_entries_from_file(filename, options.column_names,
            options.use_spark)

    tileset = make_tiles_by_index(entries, options.position.split(','), 
            options.max_zoom, options.value_field, options.importance_field,
            bins_per_dimension=options.bins_per_dimension,
            resolution=options.resolution, output_dir=options.output_dir)

    with open(op.join(options.output_dir, 'tile_info.json'), 'w') as f:
        json.dump(tileset['tileset_info'], f, indent=2)

    return


    '''
    options.min_pos = min(map(lambda x: x[options.position], entries))
    options.max_pos = max(map(lambda x: x[options.position], entries))

    return make_tiles(entries, options, zoom_level = 0,
            start_x = options.min_pos, end_x = options.max_pos)
    '''

def data_bounds(entries, num_dims):
    '''
    Get the minimum and maximum values for a data set.

    :param entries: A list of dictionaries representing the data
    :param num_dims: The number of dimensions in the data set
    :return: (mins, maxs)
    '''
    mins = [min(entries.map(lambda x: x['pos'][i]).collect()) for i in range(num_dims)]
    maxs = [max(entries.map(lambda x: x['pos'][i]).collect()) for i in range(num_dims)]

    return (mins, maxs)

def flatten(listOfLists):
    '''
    Courtesy of 

    http://stackoverflow.com/a/1077074/899470
    '''

    return list(it.chain.from_iterable(listOfLists))

def make_tiles_by_index(entries, dim_names, max_zoom, value_field='count', 
        importance_field='count', resolution=None,
        aggregate_tile=lambda tile,dim_names: tile, 
        bins_per_dimension=None, output_dir='.'):
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
    epsilon = 0.0000    # for calculating the max width so that all entries
                        # end up in the same top_level bucket

    # if the resolution is provided, we could go from the bottom up
    # and create zoom_widths that are multiples of the resolutions
    def consolidate_positions(entry):
        '''
        Place all of the dimensions in one array for this entry.
        '''
        value_field = 'count'
        importance_field = 'count'

        new_entry = {'pos': map(lambda dn: float(entry[dn]), dim_names),
                      value_field: float(entry[value_field]),
                      importance_field: float(entry[importance_field]) }
        return new_entry

    entries = entries.map(consolidate_positions)

    # O(n) get the maximum and minimum bounds of the data set
    (mins, maxs) = data_bounds(entries, len(dim_names))
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
    zoom_widths = map(lambda x: max_width / 2 ** x, zoom_levels)
    zoom_level_widths = zip(zoom_levels, zoom_widths)

    def place_in_tiles(entry):
        '''
        Place this entry into a set of tiles.
        
        :param entry: A data entry.
        '''
        values = []
        for zoom_level, zoom_width in zoom_level_widths:
            tile_pos = tuple( [zoom_level] + 
                    map(lambda (i, mind): int((entry['pos'][i] - mind) / zoom_width),
                           enumerate(mins)))
            
            ## We can actually place the tile in a bin right here and now
            bin_width = zoom_width / bins_per_dimension
            tile_mins = map(lambda (z,(m,x)): m + x * (max_width / 2 ** z), 
                    it.izip(it.cycle([tile_pos[0]]), zip(mins, tile_pos[1:])))

            bin_pos = map(lambda (i, mind): int((entry['pos'][i] - mind) / bin_width),
                          enumerate(tile_mins))

            bin_dict = col.defaultdict(int)
            bin_dict[tuple(bin_pos)] = entry[value_field]
            values += [((tile_pos), bin_dict  )]

        return values

    def reduce_bins(bins_a, bins_b):
        for bin_pos in bins_b:
            bins_a[bin_pos] += bins_b[bin_pos]
        return bins_a
    
    def aggregate_bins_seq(s, (bin_pos, bin_val)):
        s[bin_pos] += bin_val

        return s

    def aggregate_bins_comb(s1, s2):
        for (bin_pos, bin_val) in s2:
            s1[bin_pos] += bin_val

        return s1


    # place each entry into a tile
    # spark equivalent: flatmap
    # so now we have a list like this: [((0,0,0), {'pos1':1, 'pos2':2, 'count':3}), ...]
    #tile_entries = flatten(map(place_in_tiles, entries))
    tile_entries = entries.flatMap(place_in_tiles)
    #print "entries.count:", entries.count()
    #print "tile_entries.count():", tile_entries.count()
    '''
    tiles_aggregated = tile_entries.aggregateByKey(col.defaultdict(int),
            aggregate_bins_seq, aggregate_bins_comb)
    '''
    tiles_aggregated = tile_entries.reduceByKey(reduce_bins)

    # add the tile meta-data
    def add_tile_metadata((tile_id, tile_entries_iterator)):
        '''
        Add the tile's start and end data positions.
        '''
        z = tile_id[0]
        tile_width = max_width / 2 ** z
        bin_width = tile_width / bins_per_dimension

        # calculate where the tile values start along each dimension
        tile_start_pos = map(lambda (z,(m,x)): m + x * tile_width, 
                it.izip(it.cycle([tile_id[0]]), zip(mins, tile_id[1:])))

        tile_end_pos = map(lambda (z,(m,x)): m + (x+1) * tile_width, 
                it.izip(it.cycle([tile_id[0]]), zip(mins, tile_id[1:])))

        shown = []
        for (bin_pos, bin_val) in tile_entries_iterator.items():
            pos = map(lambda(md, x): md + x * bin_width, zip(tile_start_pos, bin_pos))
            shown += [{'pos': pos, value_field : bin_val}]

        # caclulate where the tile values end along each dimension
        '''
        tile_data = {'shown': shown,
                     'zoom': tile_id[0],
                     'tile_start_pos': tile_start_pos,
                     'tile_end_pos': tile_end_pos}
        '''
        tile_data = shown

        return (tile_id, tile_data)

    tiles_with_meta = tiles_aggregated.map(add_tile_metadata)

    tileset_info = {}

    def reduce_max(a,b):
        return max(a,b)

    def reduce_min(a,b):
        return min(a,b)

    
    tileset_info['max_importance'] = entries.map(lambda x: x[importance_field]).reduce(reduce_max)
    tileset_info['min_importance'] = entries.map(lambda x: x[importance_field]).reduce(reduce_min)

    tileset_info['max_value'] = entries.map(lambda x: x[value_field]).reduce(reduce_max)
    tileset_info['min_value'] = entries.map(lambda x: x[value_field]).reduce(reduce_min)

    tileset_info['min_pos'] = mins
    tileset_info['max_pos'] = maxs

    tileset_info['max_zoom'] = max_zoom
    tileset_info['max_width'] = max_width


    def save_tile(tile):
        key = tile[0]
        tile_value = tile[1]

        outpath = op.join(output_dir, '/'.join(map(str, key)) + '.json.gz')
        outdir = op.dirname(outpath)

        if not op.exists(outdir):
            os.makedirs(outdir)

        with gzip.open(outpath, 'w') as f:
            f.write(json.dumps(tile_value, indent=2))

    tiles_with_meta.foreach(save_tile)

    return {"tileset_info": tileset_info}

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

    parser.add_argument('-i', '--importance', dest='importance_field', default='importance_field',
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
    parser.add_argument('-m', '--max-zoom', dest='max_zoom', default=5,
            help='The maximum zoom level', type=int)
    parser.add_argument('--min-pos', dest='min_pos', default=None,
            help='The minimum x position', type=float)
    parser.add_argument('--max-pos', dest='max_pos', default=None,
            help='The maximum x position', type=float)
    parser.add_argument('-o', '--output-dir', help='The directory to place the tiles',
                        required=True)
    parser.add_argument('--min-value', help='The field which will be used to determinethe minimum value for any data point', default='min_y')
    parser.add_argument('--max-value', help='The field which will be used to determine the maximum value for any data point', default='max_y')

    args = parser.parse_args()

    make_tiles_from_file(args.input_file, args)

if __name__ == '__main__':
    main()


