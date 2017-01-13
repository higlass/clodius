#!/usr/bin/python

from __future__ import print_function

import pyBigWig as pbw
import negspy.coordinates as nc
import clodius.tiles as ct
import h5py
import math
import numpy as np
import os
import os.path as op
import pandas as pd
import sys
import time
import argparse

def reduce_data(data_array):
    s = set(data_array)
    lookup_table = dict([(x,i) for i,x in enumerate(s)])

    print("len lookup_table:", len(lookup_table), "len data_array:", len(data_array))
    t1 = time.time()
    new_data = [lookup_table[d] for d in data_array]
    t2 = time.time()
    
    print("Set size:", len(s), 'data size:', len(data_array))
    print("time taken:", 1000000 * (t2 - t1) / len(data_array))
    return (np.array(new_data), list(s))


def main():
    parser = argparse.ArgumentParser(description="""
    
    python main.py
""")

    parser.add_argument('filepath')
    parser.add_argument('-c', '--chunk-size', default=14, type=int)
    parser.add_argument('-z', '--zoom-step', default=8, type=int)
    parser.add_argument('-t', '--tile-size', default=1024, type=int)
    parser.add_argument('-o', '--output-file', default=None)
    parser.add_argument('-a', '--assembly', default='hg19')

    args = parser.parse_args()
    last_end = 0
    data = []

    if args.output_file is None:
        args.output_file = op.splitext(args.filepath)[0] + '.hitile'

    print("output file:", args.output_file)

    # Override the output file if it existts
    if op.exists(args.output_file):
        os.remove(args.output_file)
    f = h5py.File(args.output_file, 'w')

    # get the information about the chromosomes in this assembly
    chrom_info = nc.get_chrominfo(args.assembly)
    chrom_order = nc.get_chromorder(args.assembly)
    assembly_size = chrom_info.total_length

    tile_size = args.tile_size
    chunk_size = tile_size * 2**args.chunk_size     # how many values to read in at once while tiling

    dsets = []     # data sets at each zoom level

    # initialize the arrays which will store the values at each stored zoom level
    z = 0
    positions = []   # store where we are at the current dataset
    data_buffers = [[]]

    while assembly_size / 2 ** z > tile_size:
        dsets += [f.create_dataset('values_' + str(z), (assembly_size / 2 ** z,), dtype='f',compression='gzip')]
        data_buffers += [[]]
        positions += [0]
        z += args.zoom_step

    # load the bigWig file
    print("filepath:", args.filepath)
    bwf = pbw.open(args.filepath) 

    # store some meta data
    d = f.create_dataset('meta', (1,), dtype='f')

    d.attrs['zoom-step'] = args.zoom_step
    d.attrs['max-length'] = assembly_size
    d.attrs['assembly'] = args.assembly
    d.attrs['chrom-names'] = bwf.chroms().keys()
    d.attrs['chrom-sizes'] = bwf.chroms().values()
    d.attrs['chrom-order'] = chrom_order
    d.attrs['tile-size'] = tile_size
    d.attrs['max-zoom'] = max_zoom =  math.ceil(math.log(d.attrs['max-length'] / tile_size) / math.log(2))
    d.attrs['max-width'] = tile_size * 2 ** max_zoom

    print("assembly size (max-length)", d.attrs['max-length'])
    print("max-width", d.attrs['max-width'])
    print("max_zoom:", d.attrs['max-zoom'])
    print("chunk-size:", chunk_size)
    print("chrom-order", d.attrs['chrom-order'])

    t1 = time.time()

    for chrom in nc.get_chromorder(args.assembly):
        if chrom not in bwf.chroms():
            print("skipping chrom (not in bigWig file):", chrom)
            continue

        counter = 0
        chrom_size = bwf.chroms()[chrom]

        while counter < chrom_size:
            remaining = min(chunk_size, chrom_size - counter)
            values = bwf.values(chrom, counter, counter + remaining)
            #print("counter:", counter, "remaining:", remaining, "counter + remaining:", counter + remaining)
            #print("values:", values)

            counter += remaining
            curr_zoom = 0
            data_buffers[0] += values
            curr_time = time.time() - t1
            percent_progress = (positions[curr_zoom] + 1) / float(assembly_size)
            print("progress: {:.2f} elapsed: {:.2f} remaining: {:.2f}".format(percent_progress,
                curr_time, curr_time / (percent_progress) - curr_time))

            while len(data_buffers[curr_zoom]) >= chunk_size:
                # get the current chunk and store it, converting nans to 0
                curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
                curr_chunk[np.isnan(curr_chunk)] = 0
                dsets[curr_zoom][positions[curr_zoom]:positions[curr_zoom]+chunk_size] = curr_chunk

                # aggregate and store aggregated values in the next zoom_level's data
                data_buffers[curr_zoom+1] += list(ct.aggregate(curr_chunk, 2 ** args.zoom_step))
                data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
                positions[curr_zoom] += chunk_size
                data = data_buffers[curr_zoom+1]
                curr_zoom += 1

    # store the remaining data
    print("tile_size:", tile_size, positions[0])

    while True:
        # get the current chunk and store it
        chunk_size = len(data_buffers[curr_zoom])
        curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
        dsets[curr_zoom][positions[curr_zoom]:positions[curr_zoom]+chunk_size] = curr_chunk

        print("curr_zoom:", curr_zoom, "position:", positions[curr_zoom] + len(curr_chunk))
        print("len:", [len(d) for d in data_buffers])

        # aggregate and store aggregated values in the next zoom_level's data
        data_buffers[curr_zoom+1] += list(ct.aggregate(curr_chunk, 2 ** args.zoom_step))
        data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
        positions[curr_zoom] += chunk_size
        data = data_buffers[curr_zoom+1]
        curr_zoom += 1

        # we've created enough tile levels to cover the entire maximum width
        if curr_zoom * args.zoom_step >= max_zoom:
            break

    # still need to take care of the last chunk

    data = np.array(data)
    t1 = time.time()

    '''
    curr_zoom = 0
    dsets = []
    while len(data) > tile_size:
        (to_store_data, values_list) = reduce_data(data)
        if len(values_list) < 2 **16 and len(data) > 2**20:
            print "storing lookup"
            dsets += [f.create_dataset("zoom_" + str(curr_zoom), (len(data),), dtype='i2', compression='gzip')]
            lookup_dset = f.create_dataset("values_" + str(curr_zoom), (len(values_list), ), dtype='f')
            lookup_dset[:] = np.array(values_list)

            dsets[curr_zoom][:len(data)] = to_store_data
        else:
            print "storing raw...."
            dsets += [f.create_dataset(str(curr_zoom), (len(data),), dtype='f4', compression='gzip')]
            dsets[curr_zoom][:len(data)] = data

        new_data = ct.aggregate(data, 32)
        data = new_data
        curr_zoom += 1

    dsets += [f.create_dataset(str(curr_zoom), (len(data),), dtype='i2', compression='gzip')]
    dsets[curr_zoom][:len(data)] = data

    print "time:", time.time() - t1
    print "len(data):", len(data)
    '''

    #print data

if __name__ == '__main__':
    main()


