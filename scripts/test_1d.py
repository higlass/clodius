#!/usr/bin/python

from __future__ import print_function

import argparse
import clodius.hdf_tiles as ch
import clodius.tiles as ct
import h5py
import math
import numpy as np
import random
import sys
import time


def main():
    parser = argparse.ArgumentParser(description="""
    
    python read.py hdf_file
""")

    parser.add_argument('filepath')
    parser.add_argument('-z', default=None, type=int)
    parser.add_argument('-x', default=None, type=int)

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

    if args.x is not None and args.z is not None:
        d =  ch.get_data(f, args.z, args.x)
        print("z:", args.z, "x:", args.x, "len:", len(d), d)
        return


    for i in range(args.num_trials):
        z  = random.randint(0, int(f['meta'].attrs['max-zoom']))
        x = random.randint(0, 2**z)

        d =  ch.get_data(f, z, x)
        print("z:", z, "x:", x, "len:", len(d), d)
        #d =  ch.get_data(f, 1, 1)

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
