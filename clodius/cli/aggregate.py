# -*- coding: utf-8 -*-
from __future__ import division, print_function

from . import cli

import click
import clodius.tiles as ct
import h5py
import math
import negspy.coordinates as nc
import numpy as np
import os
import os.path as op
import pyBigWig as pbw
import time

@cli.group()
def aggregate():
    '''
    Aggregate a data file so that it stores the data at multiple
    resolutions.
    '''
    pass


def _bigwig(filepath, chunk_size=14, zoom_step=8, tile_size=1024, output_file=None, assembly='hg19', chromosome=None):
    last_end = 0
    data = []

    if output_file is None:
        if chromosome is None:
            output_file = op.splitext(filepath)[0] + '.hitile'
        else:
            output_file = op.splitext(filepath)[0] + '.' + chromosome + '.hitile'

    print("output file:", output_file)

    # Override the output file if it existts
    if op.exists(output_file):
        os.remove(output_file)
    f = h5py.File(output_file, 'w')

    # get the information about the chromosomes in this assembly
    chrom_info = nc.get_chrominfo(assembly)
    chrom_order = nc.get_chromorder(assembly)
    assembly_size = chrom_info.total_length

    tile_size = tile_size
    chunk_size = tile_size * 2**chunk_size     # how many values to read in at once while tiling

    dsets = []     # data sets at each zoom level

    # initialize the arrays which will store the values at each stored zoom level
    z = 0
    positions = []   # store where we are at the current dataset
    data_buffers = [[]]

    while assembly_size / 2 ** z > tile_size:
        dsets += [f.create_dataset('values_' + str(z), (assembly_size / 2 ** z,), dtype='f',compression='gzip')]
        data_buffers += [[]]
        positions += [0]
        z += zoom_step

    # load the bigWig file
    bwf = pbw.open(filepath)

    # store some meta data
    d = f.create_dataset('meta', (1,), dtype='f')

    if chromosome is not None:
        assembly_size = bwf.chroms()[chromosome]

    print("assembly_size:", assembly_size)

    d.attrs['zoom-step'] = zoom_step
    d.attrs['max-length'] = assembly_size
    d.attrs['assembly'] = assembly
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

    # Do we only want values from a single chromosome?
    if chromosome is not None:
        chroms_to_use = [chromosome]
    else:
        chroms_to_use = nc.get_chromorder(assembly)

    for chrom in chroms_to_use:
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
                #print("setting:", curr_zoom, positions[curr_zoom], positions[curr_zoom]+chunk_size)

                # aggregate and store aggregated values in the next zoom_level's data
                data_buffers[curr_zoom+1] += list(ct.aggregate(curr_chunk, 2 ** zoom_step))
                data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
                positions[curr_zoom] += chunk_size
                data = data_buffers[curr_zoom+1]
                curr_zoom += 1

    # store the remaining data

    while True:
        # get the current chunk and store it
        chunk_size = len(data_buffers[curr_zoom])
        curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
        curr_chunk[np.isnan(curr_chunk)] = 0
        dsets[curr_zoom][positions[curr_zoom]:positions[curr_zoom]+chunk_size] = curr_chunk
        '''
        print("setting1:", curr_zoom, positions[curr_zoom], positions[curr_zoom]+chunk_size)
        print("curr_chunk:", curr_chunk)

        print("curr_zoom:", curr_zoom, "position:", positions[curr_zoom] + len(curr_chunk))
        print("len:", [len(d) for d in data_buffers])
        '''

        # aggregate and store aggregated values in the next zoom_level's data
        data_buffers[curr_zoom+1] += list(ct.aggregate(curr_chunk, 2 ** zoom_step))
        data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
        positions[curr_zoom] += chunk_size
        data = data_buffers[curr_zoom+1]
        curr_zoom += 1

        # we've created enough tile levels to cover the entire maximum width
        if curr_zoom * zoom_step >= max_zoom:
            break

    # still need to take care of the last chunk

    data = np.array(data)
    t1 = time.time()
    pass

@aggregate.command()
@click.argument(
        'filepath',
        metavar='FILEPATH'
        )
@click.option(
        '--chunk-size',
        '-c',
        help='How many values to aggregate at once.'
             'Specified as a power of two multiplier of the tile'
             'size',
        default=14)
@click.option(
        '--zoom-step',
        '-z',
        help="The number of intermediate aggregation levels to"
             "omit",
        default=8)
@click.option(
        '--tile-size',
        '-t',
        default=1024,
        help="The number of data points in each tile."
             "Used to determine the number of zoom levels"
             "to create.")
@click.option(
        '--output-file',
        '-o',
        default=None,
        help="The default output file name to use. If this isn't"
             "specified, clodius will replace the current extension"
             "with .hitile"
        )
@click.option(
        '--assembly',
        '-a',
        help='The genome assembly that this file was created against',
        default='hg19')
@click.option(
        '--chromosome',
        default=None,
        help="Only extract values for a particular chromosome."
             "Use all chromosomes if not set."
             )
def bigwig(filepath, chunk_size, zoom_step, tile_size, output_file, assembly, chromosome):
    _bigwig(filepath, chunk_size, zoom_step, tile_size, output_file, assembly, chromosome)
