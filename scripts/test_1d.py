#!/usr/bin/python

from __future__ import print_function

import argparse
import clodius.tiles as ct
import h5py
import math
import numpy as np
import random
import sys
import time
import scipy.weave as weave

def get_data(hdf_file, z, x):

    # is the title within the range of possible tiles
    if x > 2**z:
        print("OUT OF RIGHT RANGE")
        return []
    if x < 0:
        print("OUT OF LEFT RANGE")
        return []

    d = hdf_file['meta'] 
    tile_size = int(d.attrs['tile-size'])
    zoom_step = int(d.attrs['zoom-step'])
    max_length = int(d.attrs['max-length'])
    max_zoom = int(d.attrs['max-zoom'])

    #print("max_zoom:", max_zoom)
    rz = max_zoom - z
    #print("rz:", rz)
    tile_width = max_length / 2**z

    #print("zoom_step:", zoom_step, rz / zoom_step)
    # because we only store some a subsection of the zoom levels
    next_stored_zoom = zoom_step * math.floor(rz / zoom_step)
    zoom_offset = rz - next_stored_zoom
    #print("next_stored_zoom", next_stored_zoom, 'zoom_offset:', zoom_offset)

    # the number of entries to aggregate for each new value
    num_to_agg = 2 ** zoom_offset
    total_in_length = tile_size * num_to_agg
    #print("num_to_agg:", num_to_agg, total_in_length)

    #print("zoom_offset:", zoom_offset)
    # which positions we need to retrieve in order to dynamically aggregate
    start_pos = int((x * 2 ** zoom_offset * tile_size))
    end_pos = int(start_pos + total_in_length)
    f = hdf_file['values_' + str(int(next_stored_zoom))]

    print("start_pos:", start_pos, "end_pos:", end_pos)
    ret_array = ct.tile2(f[start_pos:end_pos], int(num_to_agg))
    return ret_array
    

def main():
    parser = argparse.ArgumentParser(description="""
    
    python read.py hdf_file
""")

    parser.add_argument('filepath')
    parser.add_argument('-z', '--zoom-step', default=1, type=int)
    parser.add_argument('-n', '--num-trials', default=1, type=int)
    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')

    args = parser.parse_args()

    f = h5py.File(args.filepath, 'r')
   
    t1 = time.time()

    if args.num_trials < 1:
        print("The number of trials needs to be greater than 0", file=sys.stderr)

    for i in range(args.num_trials):
        z  = random.randint(0, int(f['meta'].attrs['max-zoom']))
        x = random.randint(0, 2**z)

        d =  get_data(f, z, x)
        print("z:", z, "x:", x, d)
        #d =  get_data(f, 1, 1)

        #print "z:", z, "x:", x
    t2 = time.time()
    print("avg time:", (t2 - t1) / args.num_trials)

    '''
    print "max_zoom:", max_zoom

    values = f['values_0']
    print "values:", f['values_8'][:10]
    print "len:", len(f['values_8'])

    assert(f['values_0'][499999] == 500000.)
    assert(f['values_8'][0] == (256 * 257) / 2)
    assert(f['values_16'][0] == (256**2 * (256**2 + 1) / 2))

    print "1953:", f['values_8'][1952]
    '''

if __name__ == '__main__':
    main()
