#!/usr/bin/python

import argparse
import clodius.save_tiles as cst
import functools as ft
import itertools as it
import collections as col
import math
import negspy.coordinates as nc
import numpy as np
import sortedcontainers as sco
import sys
import time

import multiprocessing as mpr

sys.excepthook = cst.handle_exception


def default_tile(length):
    return np.zeros(length)


def create_tiles(
    q,
    first_lines,
    input_source,
    position_cols,
    value_pos,
    max_zoom,
    bins_per_dimension,
    tile_saver,
    expand_range,
    ignore_0,
    tileset_info,
    max_width,
    triangular=False,
    max_queue_size=40000,
    print_status=False,
):
    active_tiles = col.defaultdict(sco.SortedList)
    tile_contents = col.defaultdict(
        lambda: col.defaultdict(
            lambda: col.defaultdict(ft.partial(default_tile, length=len(value_pos)))
        )
    )
    smallest_width = max_width // (2 ** max_zoom)

    prev_line_num = None
    prev_time = time.time()
    start_time = time.time()

    calculated_tile_bin_positions = col.defaultdict(dict)
    for x in it.product(range(2), repeat=len(position_cols)):
        for y in it.product(range(bins_per_dimension), repeat=len(position_cols)):
            calculated_tile_bin_positions[x][y] = tuple(
                [
                    int((bins_per_dimension * tp + bp) // 2) % bins_per_dimension
                    for (tp, bp) in zip(x, y)
                ]
            )

    def add_to_next_tile(zoom_level, tile_position, tile_bins):
        next_tile_position = tuple([tp // 2 for tp in tile_position])
        new_tile_contents = tile_contents[zoom_level - 1][next_tile_position]

        if next_tile_position not in active_tiles[zoom_level - 1]:
            active_tiles[zoom_level - 1].add(next_tile_position)

        tile_mod_pos = tuple([tp % 2 for tp in tile_position])
        calc_bin_poss = calculated_tile_bin_positions[tile_mod_pos]

        for bin_position, bin_value in tile_bins.items():
            # old_abs_pos = [tp * bins_per_dimension + bp for (tp, bp) in zip(tile_position, bin_position)]
            # new_bin_pos = tuple([int(op / 2) % bins_per_dimension for op in old_abs_pos])
            # print "tile_position:", tile_position, bin_position
            """
            for i in range(len(tile_position)):
                tp = tile_position[i]
                bp = bin_position[i]
                new_bin_pos_list[i] = int((bins_per_dimension * tp + bp) / 2) % bins_per_dimension
            new_bin_pos = tuple(new_bin_pos_list)

            new_bin_pos = calc_bin_poss[bin_position]
            print "tp, bp:", tile_position, bin_position, new_bin_pos, calc_bin_pos
            if calc_bin_pos != new_bin_pos:
                print "Wrong calc_bin_pos!"
                sys.exit(1)
            # new_bin_pos = (0,)
            if zoom_level < 3:
                print >>sys.stderr, "adding to next tile:", zoom_level-1, next_tile_position, "new_bin_pos:", new_bin_pos
            """
            try:
                new_tile_contents[calc_bin_poss[bin_position]] += bin_value
                # print("ntc:", new_tile_contents[calc_bin_poss[bin_position]])
            except KeyError:
                print("calc_bin_poss.keys():", calc_bin_poss.keys())
                print("tile_mod_pos:", tile_mod_pos, tile_position)
                raise

    for line_num, line in enumerate(it.chain(first_lines, input_source)):
        # see if we have to expand any coordinate ranges
        # bedFiles are examples
        # chr1 100 102 0.5
        # means that we need to expand that to
        # chr1 100 101 0.5
        # chr1 101 102 0.5
        all_line_parts = []

        all_line_parts = [[p for p in line.strip().split()]]

        for line_parts in all_line_parts:
            if ignore_0:
                if line_parts[value_pos][0] == "0":
                    continue
            value = [float(line_parts[vp]) for vp in value_pos]
            # print "entry_pos:", entry_pos, value

            if print_status is not None:
                if line_num != prev_line_num and line_num % print_status == 0:
                    time_str = time.strftime("%Y-%m-%d %H:%M:%S")
                    print(
                        "current_time:",
                        time_str,
                        "line_num:",
                        line_num,
                        "time:",
                        int(1000 * (time.time() - prev_time)),
                        "total_time",
                        int(time.time() - start_time),
                    )

                prev_time = time.time()
            prev_line_num = line_num

            if triangular:
                entry_pos = sorted([float(line_parts[p - 1]) for p in position_cols])
            else:
                entry_pos = [float(line_parts[p - 1]) for p in position_cols]

            tileset_info["max_value"] = [
                max(x, y) for x, y in zip(tileset_info["max_value"], value)
            ]
            tileset_info["min_value"] = [
                min(x, y) for x, y in zip(tileset_info["min_value"], value)
            ]

            entry_poss = [entry_pos]
            if expand_range is not None:
                end_pos = int(line_parts[expand_range[1] - 1])
                # print("ep", int(entry_pos[0]), end_pos -  int(entry_pos[0]) + 1)
                for i in range(int(entry_pos[0]), end_pos):
                    new_entry = entry_pos[::]
                    new_entry[0] = i
                    entry_poss += [new_entry]

            # the bin within the tile as well as the tile position
            # place this data point in the highest resolution tile that we can
            for entry_pos in entry_poss:
                current_bin = tuple(
                    [
                        int(ep // (smallest_width // bins_per_dimension))
                        % bins_per_dimension
                        for ep in entry_pos
                    ]
                )
                current_tile = tuple([int(ep // smallest_width) for ep in entry_pos])

                # we haven't seen this tile before so we save it, in case more data points need to be placed in it
                if current_tile not in active_tiles[max_zoom]:
                    active_tiles[max_zoom].add(current_tile)

                # place the data in the tile
                tile_contents[max_zoom][current_tile][current_bin] += value

            # go through all the tiles and check which ones we're done with
            # start with the max zoom
            for zoom_level in range(0, max_zoom + 1)[::-1]:
                # print "zoom_level:", zoom_level, "active_tiles length:", len(active_tiles[zoom_level]), "total_time", int(time.time() - start_time)
                current_tile = tuple(
                    [
                        int(ep // (smallest_width * 2 ** (max_zoom - zoom_level)))
                        for ep in entry_pos
                    ]
                )

                # keep track of whether any tiles were finished at the previous level
                # we can't complete a lower zoom tile unless we complete a higher zoom one
                finished_tile = False
                while len(active_tiles[zoom_level]) > 0:
                    """
                    if zoom_level < max_zoom:
                        print "at:", zoom_level, "len(at):", len(active_tiles[zoom_level]), active_tiles[zoom_level][0], current_tile
                    """
                    if active_tiles[zoom_level][0][0] < current_tile[0]:
                        tile_position = active_tiles[zoom_level][0]
                        tile_bins = tile_contents[zoom_level][tile_position]

                        # print "tile_bins:", zoom_level, tile_position, tile_bins

                        # make sure old requests get saved before we create new ones
                        already_sleeping = False
                        while q.full():  # q.qsize() > max_queue_size:
                            if print_status:
                                if already_sleeping:
                                    sys.stdout.write(".")
                                    sys.stdout.flush()
                                else:
                                    sys.stdout.write("sleeping.")
                                    sys.stdout.flush()
                                    already_sleeping = True
                            time.sleep(0.5)
                        if already_sleeping:
                            if print_status:
                                sys.stdout.write(".\n")

                        q.put((zoom_level, active_tiles[zoom_level][0], tile_bins))
                        """
                        if zoom_level < max_zoom:
                            print "putting tile"
                        """
                        finished_tile = True

                        # print "zoom_level:", zoom_level, max_zoom
                        if zoom_level > 0:
                            add_to_next_tile(zoom_level, tile_position, tile_bins)

                        # create a lower zoom level tile

                        del tile_contents[zoom_level][tile_position]
                        del active_tiles[zoom_level][0]
                    else:
                        break

                if not finished_tile:
                    break

            """
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
            """

    for zoom_level in range(0, max_zoom + 1)[::-1]:
        for tile_position in active_tiles[zoom_level]:
            tile_bins = tile_contents[zoom_level][tile_position]

            q.put((zoom_level, tile_position, tile_bins))

            if zoom_level > 0:
                add_to_next_tile(zoom_level, tile_position, tile_bins)

    tile_saver.save_tile({"tile_id": "tileset_info", "tile_value": tileset_info})

    while not q.empty():
        # print "qsize:", q.qsize()
        time.sleep(1)

    tile_saver.flush()
    return tileset_info


def main():
    """
    python make_tiles.py input_file

    Create tiles for all of the entries in the JSON file.
    """
    parser = argparse.ArgumentParser()

    # parser.add_argument('-o', '--options', dest='some_option', default='yo', help="Place holder for a real option", type='str')
    # parser.add_argument('-u', '--useless', dest='uselesss', default=False, action='store_true', help='Another useless option')
    parser.add_argument("--min-pos", help="The minimum range for the tiling")
    parser.add_argument("--max-pos", help="The maximum range for the tiling")
    parser.add_argument("--assembly", default=None)
    parser.add_argument(
        "-r", "--resolution", help="The resolution of the data", default=None, type=int
    )
    parser.add_argument(
        "-k",
        "--position-cols",
        help="The position columns (defaults to all but the last, 1-based)",
        default=None,
    )
    parser.add_argument(
        "-v",
        "--value-pos",
        help="The value column (defaults to the last one, 1-based)",
        default=None,
        type=str,
    )
    parser.add_argument(
        "-z", "--max-zoom", help="The maximum zoom value", default=None, type=int
    )
    parser.add_argument("--expand-range", help="Expand ranges of values")
    parser.add_argument(
        "--ignore-0",
        help="Ignore ranges with a zero value",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-b",
        "--bins-per-dimension",
        default=1,
        help="The number of bins to consider in each dimension",
        type=int,
    )
    parser.add_argument(
        "-e",
        "--elasticsearch-url",
        default=None,
        help="The url of the elasticsearch database where to save the tiles",
    )
    parser.add_argument(
        "-f",
        "--columnfile-path",
        default=None,
        help="The path to the column file where to save the tiles",
    )
    parser.add_argument("-n", "--num-threads", default=4, type=int)
    parser.add_argument("--triangular", default=False, action="store_true")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--max-queue-size", default=40000, type=int)
    parser.add_argument("--print-status", default=None, type=int)

    args = parser.parse_args()

    if args.resolution is None and args.max_zoom is None:
        print("One of --resolution and --max-zoom must be set", file=sys.stderr)
        sys.exit(1)

    first_line = sys.stdin.readline()
    first_line_parts = first_line.strip().split()
    if len(first_line_parts) == 0:
        print("ERROR: no input")
        return

    if args.position_cols is not None:
        position_cols = list(map(int, args.position_cols.split(",")))
    else:
        position_cols = None

    # if specific position columns aren't specified, use all but the last column
    if position_cols is None:
        position_cols = list(range(1, len(first_line_parts)))

    if args.assembly is not None:
        mins = [1 for p in position_cols]
        maxs = [nc.get_chrominfo(args.assembly).total_length for p in position_cols]
    else:
        mins = [float(p) for p in args.min_pos.split(",")]
        maxs = [float(p) for p in args.max_pos.split(",")]

    max_width = max([b - a for (a, b) in zip(mins, maxs)])

    if args.expand_range is not None:
        expand_range = list(map(int, args.expand_range.split(",")))
    else:
        expand_range = None

    if args.max_zoom is None:
        # determine the maximum zoom level based on the domain of the data
        # and the resolution
        bins_to_display_at_max_resolution = (
            max_width // args.resolution // args.bins_per_dimension
        )
        max_max_zoom = math.ceil(
            math.log(bins_to_display_at_max_resolution) / math.log(2.0)
        )

        if max_max_zoom < 0:
            max_max_zoom = 0

        max_zoom = int(max_max_zoom)
    else:
        max_zoom = args.max_zoom

    # print("max_zoom:", max_zoom)
    max_width = args.resolution * args.bins_per_dimension * 2 ** max_zoom

    value_pos = args.value_pos

    # if there's not column designated as the value column, use the last column
    if value_pos is None:
        value_pos = [len(first_line_parts) - 1]
    else:
        value_pos = [int(vp) - 1 for vp in value_pos.split(",")]

    max_data_in_sparse = args.bins_per_dimension ** len(position_cols) // 10

    """
    if args.elasticsearch_url is not None:
        tile_saver = cst.ElasticSearchTileSaver(max_data_in_sparse,
                                                args.bins_per_dimension,
                                                num_dimensions = len(position_cols),
                                                es_path = args.elasticsearch_url)
    else:
        tile_saver = cst.EmptyTileSaver(max_data_in_sparse,
                                        args.bins_per_dimension,
                                        num_dimensions = len(position_cols))
    """

    print(
        "maxs:",
        maxs,
        "max_zoom:",
        max_zoom,
        "max_data_in_sparse:",
        max_data_in_sparse,
        "url:",
        args.elasticsearch_url,
    )

    # bin_counts = col.defaultdict(col.defaultdict(int))
    q = mpr.Queue(maxsize=args.max_queue_size)

    tilesaver_processes = []
    finished = mpr.Value("b", False)
    if args.elasticsearch_url is not None:
        tile_saver = cst.ElasticSearchTileSaver(
            max_data_in_sparse,
            args.bins_per_dimension,
            len(position_cols),
            args.elasticsearch_url,
            args.log_file,
            args.print_status,
            initial_value=[0.0 for vp in value_pos],
        )
    else:
        tile_saver = cst.ColumnFileTileSaver(
            max_data_in_sparse,
            args.bins_per_dimension,
            len(position_cols),
            args.columnfile_path,
            args.log_file,
            args.print_status,
            initial_value=[0.0 for vp in value_pos],
        )

    for i in range(args.num_threads):
        p = mpr.Process(target=cst.tile_saver_worker, args=(q, tile_saver, finished))

        p.daemon = True
        p.start()
        tilesaver_processes += [(tile_saver, p)]

    tileset_info = {
        "max_value": [0 for vp in value_pos],
        "min_value": [0 for vp in value_pos],
        "min_pos": mins,
        "max_pos": maxs,
        "max_zoom": max_zoom,
        "bins_per_dimension": args.bins_per_dimension,
        "max_width": max_width,
    }

    tile_saver.save_tile({"tile_id": "tileset_info", "tile_value": tileset_info})
    tile_saver.flush()

    try:
        tileset_info = create_tiles(
            q,
            [first_line],
            sys.stdin,
            position_cols,
            value_pos,
            max_zoom,
            args.bins_per_dimension,
            tile_saver,
            expand_range,
            args.ignore_0,
            tileset_info,
            max_width,
            args.triangular,
            args.max_queue_size,
            print_status=args.print_status,
        )
    except KeyboardInterrupt:
        for (ts, p) in tilesaver_processes:
            ts.flush()
            p.terminate()
            p.join()
        raise

    finished.value = True
    # wait for the worker processes to finish
    for (ts, p) in tilesaver_processes:
        p.join()

    print("tileset_info:", tileset_info)
    tile_saver.save_tile({"tile_id": "tileset_info", "tile_value": tileset_info})
    tile_saver.flush()


if __name__ == "__main__":
    main()
