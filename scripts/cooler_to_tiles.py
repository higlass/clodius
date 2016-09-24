#!/usr/bin/python

import argparse
import clodius.higlass_getter as chg
import clodius.save_tiles as cst
import collections as col
import cooler
import h5py
import numpy as np
import scipy.sparse as ss
import sys
import time

import multiprocessing as mpr

def recursive_generate_tiles(tile_positions, coolers_matrix, info, resolution, max_zoom_to_generate, queue = None, max_queue_size=10000):
    '''
    Recursively generate tiles from a cooler file.

    :param tile_position: A 3-tuple containing (zoom_level, x_position, y_position)
    :param filepath: The path of the cooler file
    :param info: The information about the tileset
    :param resolution: The resolution of the data in the smallest tiles (in nucleotides)
    :param max_zoom_to_generate: The maximum zoom level to create tiles for
    :param max_queue_size: The maximum size of the queue (sleep until it shrinks)
    '''
    total_put = 0
    start_time = time.time()

    while len(tile_positions) > 0:
        tile_position = tile_positions.popleft()

        zoom_level = tile_position[0]
        x_pos = tile_position[1]
        y_pos = tile_position[2]

        divisor = 2 ** zoom_level

        start1 = x_pos * info['max_width'] / divisor
        end1 = (x_pos + 1) * info['max_width'] / divisor

        start2 = y_pos * info['max_width'] / divisor
        end2 = (y_pos + 1) * info['max_width'] / divisor

        t1 = time.time()
        print("st:", start1, end1-1, start2, end2-1)
        try:
            data = chg.getData3(coolers_matrix[zoom_level], zoom_level, start1, end1-1, start2, end2-1)
        except ValueError as ve:
            print("ERROR ve:", ve, file=sys.stderr)

        data_time = time.time() - t1

        if len(data) == 0:
            continue

        df = data[data['genome_start'] >= start1]
        binsize = 2 ** (info['max_zoom'] - zoom_level) * resolution

        i = (df['genome_start'].values - start1) // binsize
        j = (df['genome_end'].values - start2) // binsize
        v = np.nan_to_num(df['balanced'].values)
        m = (end1 - start1) // binsize
        n =  (end2 - start2) // binsize

        zi = zip(zip(i,j),v)
        tile_bins = dict(zi)

        data_length = len(data)

        if queue is not None:
            total_put += 1
            print("putting:", (tile_position[0], tile_position[1:]), 
                  "total_put:", total_put, 
                  "total_time: {:d}".format(int(time.time() - start_time)),
                  "time_per_put: {:.2f}".format((time.time() -  start_time) / total_put),
                  "data_time: {:.2f}".format(data_time),
                  "tile_size:", data_length,
                  "call: ({}, {}, {}, {}, {})".format(zoom_level, start1, end1-1, start2, end2-1),
                  'qsize:', queue.qsize())
            queue.put((tile_position[0], tile_position[1:], tile_bins))
            while queue.qsize() > max_queue_size:
                time.sleep(0.5)
        ###
        # Upload the tile to the server here
        ###
        if zoom_level < max_zoom_to_generate and data_length > 0:
            tile_positions.append((zoom_level+1, 2 * x_pos, 2 * y_pos))
            tile_positions.append((zoom_level+1, 2 * x_pos, 2 * y_pos + 1))
            tile_positions.append((zoom_level+1, 2 * x_pos+1, 2 * y_pos + 1))
            tile_positions.append((zoom_level+1, 2 * x_pos+1, 2 * y_pos))

        # need to recurse into higher zoom levels
        '''
        recursive_generate_tiles((zoom_level+1, 2 * x_pos, 2 * y_pos), coolers_matrix, info, 
                resolution, max_zoom_to_generate, queue = queue)
        recursive_generate_tiles((zoom_level+1, 2 * x_pos, 2 * y_pos + 1), coolers_matrix, info, 
                resolution, max_zoom_to_generate, queue = queue)
        recursive_generate_tiles((zoom_level+1, 2 * x_pos + 1, 2 * y_pos + 1), coolers_matrix, info, 
                resolution, max_zoom_to_generate, queue = queue)
        recursive_generate_tiles((zoom_level+1, 2 * x_pos + 1, 2 * y_pos), coolers_matrix, info, 
                resolution, max_zoom_to_generate, queue = queue)
        '''

def main():
    parser = argparse.ArgumentParser(description="""
    python cooler_to_tiles.py cooler_file 

    Requires the cooler package.
""")

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')
    parser.add_argument('filepath')
    parser.add_argument('-e', '--elasticsearch-url', default=None,
                        help="The url of the elasticsearch database where to save the tiles")
    parser.add_argument('-b', '--bins-per-dimension', default=1,
                        help="The number of bins to consider in each dimension",
                        type=int)
    parser.add_argument('-f', '--columnfile-path', default=None,
                        help="The path to the column file where to save the tiles")
    parser.add_argument('--assembly', default=None)
    parser.add_argument('--log-file', default=None)
    parser.add_argument('--resolution', default=1000)
    parser.add_argument('--max-zoom', default=None, type=int)
    parser.add_argument('--num-threads', default=4, type=int)

    args = parser.parse_args()
    tileset_info = chg.getInfo(args.filepath)

    num_dimensions = 2
    bins_per_dimension = tileset_info['bins_per_dimension']
    max_data_in_sparse = bins_per_dimension ** num_dimensions / 10

    if args.elasticsearch_url is not None:    
        tile_saver = cst.ElasticSearchTileSaver(max_data_in_sparse,
                                                bins_per_dimension,
                                                es_path = args.elasticsearch_url,
                                                log_file = args.log_file,
                                                num_dimensions=num_dimensions)
    else:
        tile_saver = cst.ColumnFileTileSaver(max_data_in_sparse,
                                                bins_per_dimension,
                                                file_path = args.columnfile_path,
                                                log_file = args.log_file,
                                                num_dimensions=num_dimensions)

    ############################################################################

    if args.max_zoom is not None and args.max_zoom < tileset_info['max_zoom']:
        max_zoom_to_generate = args.max_zoom
    else:
        max_zoom_to_generate = tileset_info['max_zoom']

    coolers_matrix = {}
    queue = mpr.Queue()

    tilesaver_processes = []
    finished = mpr.Value('b', False)

    print("num_threads:", args.num_threads)
    for i in range(args.num_threads):
        p = mpr.Process(target=cst.tile_saver_worker, args=(queue, tile_saver, finished))

        p.daemon = True
        p.start()
        tilesaver_processes += [(tile_saver, p)]

    tileset_info['max_value'] = 0
    tileset_info['min_value'] = 0

    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})
    tile_saver.flush()

    try:
        with h5py.File(args.filepath) as f:
            for i in range(max_zoom_to_generate+1):
                f = h5py.File(args.filepath, 'r')

                c = cooler.Cooler(f[str(i)])
                matrix = c.matrix(balance=True, as_pixels=True, join=True)

                coolers_matrix[i] = {'cooler': c, 'matrix': matrix}

            recursive_generate_tiles(col.deque([(0,0,0)]), coolers_matrix, tileset_info, 
                    args.resolution, max_zoom_to_generate, queue)
    except KeyboardInterrupt:
        print("kb interrupt:")
        for (ts, p) in tilesaver_processes:
            p.terminate()
            p.join()
            print("finished")
        raise

    finished.value = True
    # wait for the worker processes to finish
    for (ts, p) in tilesaver_processes:
        p.join()

    print("tileset_info:", tileset_info)
    tile_saver.save_tile({'tile_id': 'tileset_info', 
                          'tile_value': tileset_info})
    tile_saver.flush()


if __name__ == '__main__':
    main()


