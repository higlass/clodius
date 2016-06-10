#!/usr/bin/python

import argparse
import clodius.save_tiles as cst
import itertools as it
import collections as col
import math
import os
import os.path as op
import signal
import sortedcontainers as sco
import sys
import time
import thread
import Queue

import multiprocessing as mpr
import traceback

def handle_exception(exc_type, exc_value, exc_traceback):
    print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    os._exit(1)

sys.excepthook = handle_exception

def tile_saver_worker(q, tile_saver, finished):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while q.qsize() > 0 or (not finished.value):
        #print "working...", q.qsize()
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


def create_tiles(q, first_lines, input_source, position_cols, value_pos, max_zoom, 
        bins_per_dimension, tile_saver, expand_range, ignore_0, tileset_info, max_width):
    active_tiles = col.defaultdict(sco.SortedList)
    max_data_in_sparse = bins_per_dimension ** len(position_cols) / 5.
    tile_contents = col.defaultdict(lambda: col.defaultdict(lambda: col.defaultdict(int)))
    smallest_width = max_width / (2 ** max_zoom)

    prev_line_num = None
    prev_time = time.time()
    start_time = time.time()

    def add_to_next_tile(zoom_level, tile_position, tile_bins):
        next_tile_position = tuple([tp / 2 for tp in tile_position])

        if next_tile_position not in active_tiles[zoom_level - 1]:
            active_tiles[zoom_level - 1].add(next_tile_position)

        for bin_position in tile_bins:
            old_abs_pos = [tp * bins_per_dimension + bp for (tp, bp) in zip(tile_position, bin_position)]
            #new_bin_pos = tuple([int(op / 2) % bins_per_dimension for op in old_abs_pos])
            new_bin_pos = tuple([ int((bins_per_dimension * tp + bp) / 2) % bins_per_dimension for  (tp, bp) in zip(tile_position, bin_position)])

            tile_contents[zoom_level-1][next_tile_position][new_bin_pos] += tile_bins[bin_position]

            #print "old_bin_pos:", bin_position
            #print "next_tile_position:", next_tile_position, "new_bin_pos:", new_bin_pos

    for line_num,line in enumerate(it.chain(first_lines, input_source)):
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
            if ignore_0:
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
            #print "entry_pos:", entry_pos

            tileset_info['max_value'] = max(tileset_info['max_value'], value)
            tileset_info['min_value'] = min(tileset_info['min_value'], value)

            if line_num != prev_line_num and line_num % 10000 == 0:
                time_str = time.strftime("%Y-%m-%d %H:%M:%S")
                print "current_time:", time_str, "line_num:", line_num, "time:", int(1000 * (time.time() - prev_time)), "qsize:", q.qsize(), "total_time", int(time.time() - start_time)

                prev_time = time.time()
            prev_line_num = line_num

            # the bin within the tile as well as the tile position
            # place this data point in the highest resolution tile that we can
            current_bin = tuple([int(ep / (smallest_width / bins_per_dimension)) % bins_per_dimension for ep in entry_pos])
            current_tile = tuple([int(ep / smallest_width) for ep in entry_pos])

            # we haven't seen this tile before so we save it, in case more data points need to be placed in it
            if current_tile not in active_tiles[max_zoom]:
                active_tiles[max_zoom].add(current_tile)

            # place the data in the tile
            tile_contents[max_zoom][current_tile][current_bin] += value
            
            # go through all the tiles and check which ones we're done with
            # start with the max zoom
            for zoom_level in range(0, max_zoom+1)[::-1]:
                #print "zoom_level:", zoom_level, "active_tiles length:", len(active_tiles[zoom_level]), "total_time", int(time.time() - start_time)
                current_tile = tuple([int(ep / (smallest_width * 2 ** (max_zoom - zoom_level)) ) for ep in entry_pos])

                # keep track of whether any tiles were finished at the previous level
                # we can't complete a lower zoom tile unless we complete a higher zoom one
                finished_tile = False
                while len(active_tiles[zoom_level]) > 0:
                    '''
                    if zoom_level < max_zoom:
                        print "at:", zoom_level, "len(at):", len(active_tiles[zoom_level]), active_tiles[zoom_level][0], current_tile
                    '''
                    if active_tiles[zoom_level][0][0] < current_tile[0]:
                        tile_position = active_tiles[zoom_level][0]
                        tile_value = tile_contents[zoom_level][tile_position]
                        tile_bins = tile_contents[zoom_level][tile_position]

                        #print "tile_bins:", zoom_level, tile_position, tile_bins

                        # make sure old requests get saved before we create new ones
                        while q.qsize() > 400000:
                            print "sleepin..."
                            time.sleep(0.25)

                        q.put((zoom_level, active_tiles[zoom_level][0], tile_bins))
                        '''
                        if zoom_level < max_zoom:
                            print "putting tile"
                        '''
                        finished_tile = True

                        #print "zoom_level:", zoom_level, max_zoom
                        if zoom_level > 0:
                            add_to_next_tile(zoom_level, tile_position, tile_bins)

                        # create a lower zoom level tile

                        del tile_contents[zoom_level][tile_position]
                        del active_tiles[zoom_level][0]
                    else:
                        break

                if not finished_tile:
                    break

            '''
            for zoom_level, tile_width in zip(range(0, max_zoom+1), tile_widths):


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
            '''

    for zoom_level in range(0, max_zoom+1)[::-1]:
        for tile_position in active_tiles[zoom_level]:
            tile_value = tile_contents[zoom_level][tile_position]
            tile_bins = tile_contents[zoom_level][tile_position]

            q.put((zoom_level, tile_position, tile_bins))
            
            if zoom_level > 0:
                add_to_next_tile(zoom_level, tile_position, tile_bins)

    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})


    while q.qsize() > 0:
        print "qsize:", q.qsize()
        time.sleep(1)

    tile_saver.flush()
    return tileset_info

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


        if max_max_zoom < 0:
            max_max_zoom = 0

        max_zoom = int(max_max_zoom)
    else:
        max_zoom = args.max_zoom

    max_width = args.resolution * args.bins_per_dimension * 2 ** max_zoom
    smallest_width = args.resolution * args.bins_per_dimension

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

    print "max_data_in_sparse:", max_data_in_sparse
    print "max_zoom:", max_zoom


    #bin_counts = col.defaultdict(col.defaultdict(int))
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

        p.daemon = True
        p.start()
        tilesaver_processes += [(tile_saver, p)]

    tileset_info = {'max_value': 0,
                    'min_value': 0,
                    'min_pos': mins,
                    'max_pos': maxs,
                    'max_zoom': max_zoom,
                    'bins_per_dimension': args.bins_per_dimension,
                    'max_width': max_width}


    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})
    tile_saver.flush()

    try:
        tileset_info = create_tiles(q, [first_line], sys.stdin, position_cols, value_pos, 
                max_zoom, args.bins_per_dimension, tile_saver, expand_range,
                args.ignore_0, tileset_info, max_width)
    except KeyboardInterrupt:
        print "kb interrupt:"
        for (ts, p) in tilesaver_processes:
            p.terminate()
            p.join()
            print "finished"
        raise

    # wait for the worker processes to finish
    for (ts, p) in tilesaver_processes:
        p.terminate()
        p.join()

    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})
    tile_saver.flush()
         

if __name__ == '__main__':
    main()


