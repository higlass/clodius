#!/usr/bin/python

import csv
import dask.array as da
import h5py
import math
import numpy as np
import argparse
import time


def coarsen(f, tile_size=256):
    '''
    Create data pyramid.
    '''
    grid = f['resolutions']['1']['values']
    top_n = grid.shape[0]
    max_zoom = math.ceil(math.log(top_n / tile_size) / math.log(2))

    chunk_size = tile_size * 16
    curr_size = grid.shape
    dask_dset = da.from_array(grid, chunks=(chunk_size, chunk_size))

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


def parse(input_handle, output_hdf5, height, width,
          delimiter, first_n, is_square, is_labelled):
    reader = csv.reader(input_handle, delimiter=delimiter)
    if is_labelled:
        first_row = next(reader)
        labels = first_row[1:(first_n + 1) if first_n else None]
        if is_square:
            output_hdf5.create_dataset(
                'labels',
                data=np.array(labels, dtype=h5py.special_dtype(vlen=str)),
                compression='lzf')
        # TODO: Handle non-square labels
        # https://github.com/higlass/clodius/issues/68

    tile_size = 256
    limit = max(height, width)
    max_zoom = math.ceil(math.log(limit / tile_size) / math.log(2))
    max_width = tile_size * 2 ** max_zoom

    g = output_hdf5.create_group('resolutions')
    g1 = g.create_group('1')
    ds = g1.create_dataset('values', (max_width, max_width),
                           dtype='f4', compression='lzf', fillvalue=np.nan)
    g1.create_dataset('nan_values', (max_width, max_width),
                      dtype='f4', compression='lzf', fillvalue=0)
    # TODO: We don't write to this... Is it necessary?

    start_time = time.time()
    counter = 0
    for row in reader:
        x = np.array([float(p) for p in row[1 if is_labelled else None:]])
        ds[counter, :len(x)] = x

        counter += 1
        if counter == first_n:
            break

        time_elapsed = time.time() - start_time
        time_per_entry = time_elapsed / counter

        time_remaining = time_per_entry * (height - counter)
        print("counter:", counter, "sum(x):", sum(x),
              "time remaining: {:d} seconds".format(int(time_remaining)))

    coarsen(output_hdf5)
    output_hdf5.close()


def get_height(input_path, is_labelled=True):
    '''
    We need to scan the file once just to see how many lines it contains.
    If it is tall and narrow, the first tile will need to be larger than just
    looking at the width of the first row would suggest.
    '''
    with open(input_path) as f:
        for i, l in enumerate(f):
            pass
    if is_labelled:
        return i
    else:
        return i + 1


def get_width(input_path, is_labelled, delimiter='\t'):
    '''
    Assume the number of elements in the first row is the total width.
    '''
    with open(input_path, 'r', newline='') as input_handle:
        reader = csv.reader(input_handle, delimiter=delimiter)
        len_row = len(next(reader))
        if is_labelled:
            return len_row - 1
        return len_row


def main():
    parser = argparse.ArgumentParser(description='''
        Given a tab-delimited file, produces an HDF5 file with mrmatrix
        ("multi-resolution matrix") structure: Under the "resolutions"
        group are datasets, named with successive powers of 2,
        which represent successively higher aggregations of the input.
    ''')
    parser.add_argument('input_file', help='TSV file path')
    parser.add_argument('output_file', help='HDF5 file')
    parser.add_argument('-d', '--delimiter', type=str, default='\t',
                        metavar='D', help='Delimiter; defaults to tab')
    parser.add_argument('-n', '--first-n', type=int, default=None, metavar='N',
                        help='Only read first N columns from first N rows')
    parser.add_argument('-s', '--square', action='store_true',
                        help='Row labels are assumed to match column labels')
    parser.add_argument('-l', '--labelled', action='store_true',
                        help='TSV Matrix has column and row labels')
    args = parser.parse_args()

    height = get_height(args.input_file, is_labelled=args.labelled)
    width = get_width(args.input_file, is_labelled=args.labelled,
                      delimiter=args.delimiter
    print('height:', height)
    print('width:', width)

    f_in = open(args.input_file, 'r', newline='')

    parse(f_in,
          h5py.File(args.output_file, 'w'),
          height=height, width=width,
          delimiter=args.delimiter,
          first_n=args.first_n,
          is_square=args.square,
          is_labelled=args.labelled)

    f = h5py.File(args.output_file, 'r')
    print("sum1:", np.nansum(f['resolutions']['1']['values'][0]))


if __name__ == '__main__':
    main()
