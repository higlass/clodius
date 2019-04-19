#!/usr/bin/python

import dask.array as da
import h5py
import math
import numpy as np
import os
import os.path as op
import sys
import argparse
import time

def coarsen(f):
    '''
    Create data pyramid.
    '''
    grid = f['resolutions']['1']['values']
    top_n = grid.shape[0]
    tile_size = 256

    max_zoom = math.ceil(math.log(top_n / tile_size) / math.log(2))
    max_width = tile_size * 2 ** max_zoom
    
    chunk_size=tile_size * 16
    curr_size = grid.shape
    dask_dset = da.from_array(grid, chunks=(chunk_size,chunk_size))

    r = f['resolutions']
    curr_resolution = 1

    while curr_resolution < 2 ** max_zoom:
        curr_size = tuple(np.array(curr_size) / 2)
        print('coarsening')
        curr_resolution *= 2

        print("curr_size:", curr_size)
        g = r.create_group(str(curr_resolution))
        values = g.require_dataset('values', curr_size, dtype='f4',
            compression='lzf', fillvalue=np.nan)

        dask_dset = dask_dset.rechunk((chunk_size, chunk_size))
        dask_dset = da.coarsen(np.nansum, dask_dset, {0: 2, 1: 2})
        da.store(dask_dset, values)

def main():
    parser = argparse.ArgumentParser(description="""
    
    python tsv-dense-to-sparse
""")

    parser.add_argument('input_file')
    parser.add_argument('output_file')
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true',
    #					 help='Another useless option')
    parser.add_argument('-n', '--first-n', type=int, default=None,
            help="Only use the first n entries in the matrix")

    args = parser.parse_args()

    count = 0
    top_n = args.first_n

    if args.input_file == '-':
        f_in = sys.stdin
    else:
        f_in = open(args.input_file, 'r')

    first_line = next(f_in)
    parts = first_line.split('\t')

    if top_n is None:
        top_n = len(parts) - 1

    labels = parts[1:top_n+1]
    tile_size = 256
    max_zoom = math.ceil(math.log(top_n / tile_size) / math.log(2))
    max_width = tile_size * 2 ** max_zoom

    filepath = args.output_file
    if op.exists(filepath):
        os.remove(filepath)
    
    f = h5py.File(args.output_file, 'w')
    labels_dset = f.create_dataset('labels', data=np.array(labels, dtype=h5py.special_dtype(vlen=str)), 
            compression='lzf')

    g = f.create_group('resolutions')
    g1 = g.create_group('1')
    ds = g1.create_dataset('values', (max_width, max_width), 
            dtype='f4', compression='lzf', fillvalue=np.nan)
    ds1 = g1.create_dataset('nan_values', (max_width, max_width), 
            dtype='f4', compression='lzf', fillvalue=0)

    start_time = time.time()
    counter = 0
    for line in f_in:
        parts = line.strip().split('\t')[1:top_n+1]
        x = np.array([float(p) for p in parts])
        ds[counter,:len(x)] = x

        counter += 1
        if counter == top_n:
            break

        time_elapsed = time.time() - start_time
        time_per_entry = time_elapsed / counter

        time_remaining = time_per_entry * (top_n - counter)
        print("counter:", counter, "sum(x):", sum(x), "time remaining: {:d} seconds".format(int(time_remaining)))

    coarsen(f)

    f.close()
    
    f = h5py.File(args.output_file, 'r')
    print("sum1:", np.nansum(f['resolutions']['1']['values'][0]))

if __name__ == '__main__':
    main()


