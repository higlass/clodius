#!/usr/bin/python

import argparse
import clodius.save_tiles as cst
import itertools as it
import collections as col
import math
import os.path as op
import signal
import sortedcontainers as sco
import sys
import time
import Queue

import multiprocessing as mpr

def tile_saver_worker(q, tile_saver, finished):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while q.qsize() > 0 or (not finished.value):
        try:
            (zoom_level, tile_pos, tile_bins) = q.get(timeout=1)
            tile_saver.save_binned_tile(zoom_level,
                                        tile_pos,
                                        tile_bins)
        except (KeyboardInterrupt, SystemExit):
            print "Exiting..."
            break
        except Queue.Empty:
            tile_saver.flush()

    print "finishing", q.qsize(), tile_saver
    tile_saver.flush()

def main():
    usage = """
    python make_tiles.py input_file

    Create tiles for all of the entries in the JSON file.
    """
    parser = argparse.ArgumentParser()

    #parser.add_argument('-o', '--options', dest='some_option', default='yo', help="Place holder for a real option", type='str')
    #parser.add_argument('-u', '--useless', dest='uselesss', default=False, action='store_true', help='Another useless option')
    parser.add_argument('--min-pos', required=True,
                        help="The minimum range for the tiling")
    parser.add_argument('--max-pos', required=True,
                        help="The maximum range for the tiling")
    parser.add_argument('-r', '--resolution', help="The resolution of the data", 
                        default=None, type=int )
    parser.add_argument('-k', '--position-cols', help="The position columns (defaults to all but the last, 1-based)", default=None)
    parser.add_argument('-v', '--value-pos', help='The value column (defaults to the last one, 1-based)', default=None, type=int)
    parser.add_argument('-z', '--max-zoom', help='The maximum zoom value', default=None, type=int)
    parser.add_argument('--expand-range', help='Expand ranges of values')
    parser.add_argument('--ignore-0', help='Ignore ranges with a zero value', default=False, action='store_true')
    parser.add_argument('-b', '--bins-per-dimension', default=1,
                        help="The number of bins to consider in each dimension",
                        type=int)
    parser.add_argument('-e', '--elasticsearch-url', default=None,
                        help="The url of the elasticsearch database where to save the tiles")
    parser.add_argument('-n', '--num-threads', default=1, type=int)


    args = parser.parse_args()

    if args.resolution is None and args.max_zoom is None:
        print >>sys.stderr, "One of --resolution and --max-zoom must be set"
        sys.exit(1)

    mins = [float(p) for p in args.min_pos.split(',')]
    maxs = [float(p) for p in args.max_pos.split(',')]

    max_width = max([b - a for (a,b) in zip(mins, maxs)])

    if args.expand_range is not None:
        expand_range = map(int, args.expand_range.split(','))
    else:
        expand_range = None

    if args.position_cols is not None:
        position_cols = map(int, args.position_cols.split(','))
    else:
        position_cols = None

    if args.max_zoom is None:
        # determine the maximum zoom level based on the domain of the data
        # and the resolution
        bins_to_display_at_max_resolution = max_width / args.resolution / args.bins_per_dimension
        max_max_zoom = math.ceil(math.log(bins_to_display_at_max_resolution) / math.log(2.))

        max_width = args.resolution * args.bins_per_dimension * 2 ** max_max_zoom

        if max_max_zoom < 0:
            max_max_zoom = 0

        max_zoom = int(max_max_zoom)
    else:
        max_zoom = args.max_zoom

    value_pos = args.value_pos
    
    first_line = sys.stdin.readline()
    first_line_parts = first_line.strip().split()
    # if specific position columns aren't specified, use all but the last column
    if position_cols is None:
        position_cols = range(1,len(first_line_parts))

    # if there's not column designated as the value column, use the last column
    if value_pos is None:
        value_pos = len(first_line_parts)

    max_data_in_sparse = args.bins_per_dimension ** len(position_cols) / 5.

    '''
    if args.elasticsearch_url is not None:
        tile_saver = cst.ElasticSearchTileSaver(max_data_in_sparse,
                                                args.bins_per_dimension,
                                                num_dimensions = len(position_cols),
                                                es_path = args.elasticsearch_url)
    else:
        tile_saver = cst.EmptyTileSaver(max_data_in_sparse,
                                        args.bins_per_dimension,
                                        num_dimensions = len(position_cols))
    '''

    tile_widths = [max_width / 2 ** zl for zl in range(0, max_zoom+1)]

    print "max_data_in_sparse:", max_data_in_sparse
    print "max_zoom:", max_zoom

    active_bins = col.defaultdict(sco.SortedList)
    active_tiles = col.defaultdict(sco.SortedList)

    prev_time = time.time()

    #bin_counts = col.defaultdict(col.defaultdict(int))
    tile_contents = col.defaultdict(lambda: col.defaultdict(lambda: col.defaultdict(int)))
    q = mpr.Queue()

    tilesaver_processes = []
    finished = mpr.Value('b', False)
    tile_saver = cst.ElasticSearchTileSaver(max_data_in_sparse,
                                            args.bins_per_dimension,
                                            len(position_cols),
                                            args.elasticsearch_url)
    for i in range(args.num_threads):
        p = mpr.Process(target=tile_saver_worker, args=(q, tile_saver, finished))
        print "p:", p

        p.start()
        tilesaver_processes += [(tile_saver, p)]

    tileset_info = {'max_value': 0,
                    'min_value': 0,
                    'min_pos': mins,
                    'max_pos': maxs,
                    'max_zoom': max_zoom,
                    'bins_per_dimension': args.bins_per_dimension,
                    'max_width': max_width}


    start_time = time.time()
    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})
    tile_saver.flush()

    for line_num,line in enumerate(it.chain([first_line], sys.stdin)):
        # see if we have to expand any coordinate ranges
        # bedFiles are examples
        # chr1 100 102 0.5
        # means that we need to expand that to
        # chr1 100 101 0.5
        # chr1 101 102 0.5
        all_line_parts = []

        if expand_range is None:
            all_line_parts = [[p for p in line.strip().split()]]
        else:
            line_parts = [p for p in line.strip().split()]
            if args.ignore_0:
                if line_parts[value_pos-1] == "0":
                    continue

            for i in range(int(line_parts[expand_range[0]-1]), 
                           int(line_parts[expand_range[1]-1])):
                new_line_part = line_parts[::]
                new_line_part[expand_range[0]-1] = i
                all_line_parts += [new_line_part]

        for line_parts in all_line_parts:
            entry_pos = [float(line_parts[p-1]) for p in position_cols]
            value = float(line_parts[value_pos-1])

            tileset_info['max_value'] = max(tileset_info['max_value'], value)
            tileset_info['min_value'] = min(tileset_info['min_value'], value)

            for zoom_level, tile_width in zip(range(0, max_zoom+1), tile_widths):
                if line_num % 10000 == 0 and zoom_level == 0:
                    print "line_num:", line_num, "time:", int(1000 * (time.time() - prev_time)), "qsize:", q.qsize(), "total_time", int(time.time() - start_time)

                    prev_time = time.time()

                # the bin within the tile as well as the tile position
                current_bin = tuple([int(ep / (tile_width / args.bins_per_dimension)) % args.bins_per_dimension for ep in entry_pos])
                current_tile = tuple([int(ep / tile_width) for ep in entry_pos])

                print "current_tile:", current_tile, entry_pos, tile_width

                if current_tile not in active_tiles[zoom_level]:
                    active_tiles[zoom_level].add(current_tile)

                tile_contents[zoom_level][current_tile][current_bin] += value

                # which bins will never be touched again?
                # all bins at the current zoom level where (entry_pos[0] / tile_width) < current_bin[0]
                while len(active_tiles[zoom_level]) > 0:
                    if active_tiles[zoom_level][0][0] < current_tile[0]:
                        tile_position = active_tiles[zoom_level][0]
                        tile_value = tile_contents[zoom_level][tile_position]
                        tile_bins = tile_contents[zoom_level][tile_position]

                        while q.qsize() > 40000:
                            time.sleep(1)

                        q.put((zoom_level, active_tiles[zoom_level][0], tile_bins))

                        del tile_contents[zoom_level][tile_position]
                        del active_tiles[zoom_level][0]
                    else:
                        break


    for zoom_level in active_tiles:
        for tile_position in active_tiles[zoom_level]:
            tile_value = tile_contents[zoom_level][tile_position]
            tile_bins = tile_contents[zoom_level][tile_position]

            q.put((zoom_level, tile_position, tile_bins))

    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})

    finished.value = True

    while q.qsize() > 0:
        print "qsize:", q.qsize()
        time.sleep(1)
         
    tile_saver.flush()

if __name__ == '__main__':
    main()


