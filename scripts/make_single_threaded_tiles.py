#!/usr/bin/python

import argparse
import collections as col
import os.path as op
import sortedcontainers as sco
import sys
import time

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
    parser.add_argument('-r', '--resolution', help="The resolution of the data", default=None)
    parser.add_argument('-k', '--position-cols', help="The position columns (defaults to all but the last, 1-based)", default=None)
    parser.add_argument('-v', '--value-pos', help='The value column (defaults to the last one, 1-based)', default=None)
    parser.add_argument('-z', '--max-zoom', help='The maximum zoom value', default=None, type=int)

    args = parser.parse_args()

    if args.resolution is None and args.max_zoom is None:
        print >>sys.stderr, "One of --resolution and --max-zoom must be set"

    mins = [float(p) for p in args.min_pos.split(',')]
    maxs = [float(p) for p in args.max_pos.split(',')]

    max_width = max([b - a for (a,b) in zip(mins, maxs)])

    if args.position_cols is not None:
        position_cols = args.position_cols.split(',')
    else:
        position_cols = None

    if args.max_zoom is None:
        # determine the maximum zoom level based on the domain of the data
        # and the resolution
        pass
    else:
        max_zoom = args.max_zoom

    value_pos = args.value_pos
    tile_widths = [max_width / 2 ** zl for zl in range(0, max_zoom+1)]
    active_bins = col.defaultdict(sco.SortedList)
    prev_time = time.time()

    for line_num,line in enumerate(sys.stdin):
        line_parts = [p for p in line.strip().split()]

        if position_cols is not None:
            entry_pos = [float(line_parts[p-1]) for p in p in position_cols]
        else:
            entry_pos = [float(p) for p in line_parts[:-1]]

        if value_pos is not None:
            value = float(line_parts[value_pos-1])
        else:
            value = float(line_parts[1])

        for zoom_level, tile_width in zip(range(0, max_zoom+1), tile_widths):
            current_bin = tuple([int(ep / tile_width) for ep in entry_pos])

            if current_bin not in active_bins[zoom_level]:
                active_bins[zoom_level].add(tuple(current_bin))

            # which bins will never be touched again?
            # all bins at the current zoom level where (entry_pos[0] / tile_width) < current_bin[0]
            
            if line_num % 10000 == 0:
                print "line_num:", line_num, "time:", int(1000 * (time.time() - prev_time)), "zoom_level, tile_width", zoom_level, len(active_bins[zoom_level])

                prev_time = time.time()
            while len(active_bins[zoom_level]) > 0:
                if active_bins[zoom_level][0][0] < current_bin[0]:
                    del active_bins[zoom_level][0]
                else:
                    break

if __name__ == '__main__':
    main()


