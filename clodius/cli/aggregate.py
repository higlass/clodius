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
import pybedtools as pbt
import pyBigWig as pbw
import slugid
import time

@cli.group()
def aggregate():
    '''
    Aggregate a data file so that it stores the data at multiple
    resolutions.
    '''
    pass

def store_meta_data(cursor, zoom_step, max_length, assembly, chrom_names,
        chrom_sizes, tile_size, max_zoom, max_width):

    cursor.execute('''
        CREATE TABLE tileset_info
        (
            zoom_step INT,
            max_length INT,
            assembly text,
            chrom_names text,
            chrom_sizes text,
            tile_size REAL,
            max_zoom INT,
            max_width REAL
        )
        ''')

    cursor.execute('INSERT INTO tileset_info VALUES (?,?,?,?,?,?,?,?)',
            (zoom_step, max_length, assembly,
                "\t".join(chrom_names), "\t".join(map(str,chrom_sizes)),
                tile_size, max_zoom, max_width))
    cursor.commit()

    pass

# all entries are broked up into ((tile_pos), [entry]) tuples
# we just need to reduce the tiles so that no tile contains more than
# max_entries_per_tile entries
# (notice that [entry] is an array), this format will be important when
# reducing to the most important values
def reduce_values_by_importance(entry1, entry2, max_entries_per_tile=100, reverse_importance=False):
    def extract_key(entries):
        return [(e[-2], e) for e in entries]
    by_uid = dict(extract_key(entry1) + extract_key(entry2))
    combined_by_uid = by_uid.values()

    if reverse_importance:
        combined_entries = sorted(combined_by_uid,
                key=lambda x: float(x[-1]))
    else:
        combined_entries = sorted(combined_by_uid,
                key=lambda x: -float(x[-1]))

    byKey = {}

    return combined_entries[:max_entries_per_tile]

def _bedfile(filepath, output_file, assembly, importance_column, chromosome, max_per_tile, tile_size):
    if output_file is None:
        output_file = filepath + ".multires"
    else:
        output_file = output_file

    if op.exists(output_file):
        os.remove(output_file)

    bed_file = pbt.BedTool(filepath)

    def line_to_np_array(line):
        '''
        Convert a bed file line to a numpy array which can later
        be used as an entry in an h5py file.
        '''

        if importance_column is None:
            importance = line.stop - line.start
        elif importance_column == 'random':
            importance = random.random()
        else:
            importance = int(line.fields[int(importance_column)-1])

        # convert chromosome coordinates to genome coordinates

        genome_start = nc.chr_pos_to_genome_pos(str(line.chrom), line.start, assembly)
        genome_end = nc.chr_pos_to_genome_pos(line.chrom, line.stop, assembly)

        pos_offset = genome_start - line.start
        parts = {
                    'startPos': genome_start,
                    'endPos': genome_end,
                    'uid': slugid.nice(),
                    'chrOffset': pos_offset,
                    'fields': '\t'.join(line.fields),
                    'importance': importance,
                    'chromosome': str(line.chrom)
                    }

        return parts

    dset = [line_to_np_array(line) for line in bed_file]
    
    if chromosome is not None:
        dset = [d for d in dset if d['chromosome'] == chromosome]

    # We neeed chromosome information as well as the assembly size to properly
    # tile this data
    tile_size = tile_size
    chrom_info = nc.get_chrominfo(assembly)
    #if chromosome is None:
    assembly_size = chrom_info.total_length
    '''
    else:
        try:
            assembly_size = chrom_info.chrom_lengths[chromosome]
        except KeyError:
            print("ERROR: Chromosome {} not found in assembly {}.".format(chromosome, assembly), file=sys.stderr)
            return 1
    '''

    #max_zoom = int(math.ceil(math.log(assembly_size / min_feature_width) / math.log(2)))
    max_zoom = int(math.ceil(math.log(assembly_size / tile_size) / math.log(2)))
    '''
    if max_zoom is not None and max_zoom < max_zoom:
        max_zoom = max_zoom
    '''

    # this script stores data in a sqlite database
    import sqlite3
    conn = sqlite3.connect(output_file)

    # store some meta data
    store_meta_data(conn, 1,
            max_length = assembly_size,
            assembly = assembly,
            chrom_names = nc.get_chromorder(assembly),
            chrom_sizes = nc.get_chromsizes(assembly),
            tile_size = tile_size,
            max_zoom = max_zoom,
            max_width = tile_size * 2 ** max_zoom)

    max_width = tile_size * 2 ** max_zoom
    uid_to_entry = {}

    intervals = []

    # store each bed file entry as an interval
    for d in dset:
        uid = d['uid']
        uid_to_entry[uid] = d
        intervals += [(d['startPos'], d['endPos'], uid)]

    tile_width = tile_size

    removed = set()

    c = conn.cursor()
    c.execute(
    '''
    CREATE TABLE intervals
    (
        id int PRIMARY KEY,
        zoomLevel int,
        importance real,
        startPos int,
        endPos int,
        chrOffset int,
        uid text,
        fields text
    )
    ''')

    c.execute('''
        CREATE VIRTUAL TABLE position_index USING rtree(
            id,
            rStartPos, rEndPos
        )
        ''')

    curr_zoom = 0
    counter = 0

    max_viewable_zoom = max_zoom

    if max_zoom is not None and max_zoom < max_zoom:
        max_viewable_zoom = max_zoom

    while curr_zoom <= max_viewable_zoom and len(intervals) > 0:
        # at each zoom level, add the top genes
        tile_width = tile_size * 2 ** (max_zoom - curr_zoom)

        for tile_num in range(max_width // tile_width):
            # go over each tile and distribute the remaining values
            #values = interval_tree[tile_num * tile_width: (tile_num+1) * tile_width]
            from_value = tile_num * tile_width
            to_value = (tile_num + 1) * tile_width
            entries = [i for i in intervals if (i[0] < to_value and i[1] > from_value)]
            values_in_tile = sorted(entries,
                    key=lambda x: -uid_to_entry[x[-1]]['importance'])[:max_per_tile]   # the importance is always the last column
                                                            # take the negative because we want to prioritize
                                                            # higher values

            if len(values_in_tile) > 0:
                for v in values_in_tile:
                    counter += 1

                    value = uid_to_entry[v[-1]]

                    # one extra question mark for the primary key
                    exec_statement = 'INSERT INTO intervals VALUES (?,?,?,?,?,?,?,?)'
                    ret = c.execute(
                            exec_statement,
                            # primary key, zoomLevel, startPos, endPos, chrOffset, line
                            (counter, curr_zoom,
                                value['importance'],
                                value['startPos'], value['endPos'],
                                value['chrOffset'],
                                value['uid'],
                                value['fields'])
                            )
                    conn.commit()

                    exec_statement = 'INSERT INTO position_index VALUES (?,?,?)'
                    ret = c.execute(
                            exec_statement,
                            (counter, value['startPos'], value['endPos'])  #add counter as a primary key
                            )
                    conn.commit()
                    intervals.remove(v)
        #print ("curr_zoom:", curr_zoom, file=sys.stderr)
        curr_zoom += 1

    conn.commit()

    conn.close()


    return


def _bigwig(filepath, chunk_size=14, zoom_step=8, tile_size=1024, output_file=None, assembly='hg19', chromosome=None):
    last_end = 0
    data = []

    if output_file is None:
        if chromosome is None:
            output_file = op.splitext(filepath)[0] + '.hitile'
        else:
            output_file = op.splitext(filepath)[0] + '.' + chromosome + '.hitile'

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


    # load the bigWig file
    bwf = pbw.open(filepath)

    # store some meta data
    d = f.create_dataset('meta', (1,), dtype='f')

    if chromosome is not None:
        d.attrs['min-pos'] = chrom_info.cum_chrom_lengths[chromosome]
        d.attrs['max-pos'] = chrom_info.cum_chrom_lengths[chromosome] + bwf.chroms()[chromosome]
    else:
        d.attrs['min-pos'] = 0
        d.attrs['max-pos'] = assembly_size

    data_size = d.attrs['max-pos'] - d.attrs['min-pos'] + 1

    while data_size / 2 ** z > tile_size:
        dsets += [f.create_dataset('values_' + str(z), (math.ceil(data_size / 2 ** z),), dtype='f',compression='gzip')]
        data_buffers += [[]]
        positions += [0]
        z += zoom_step

    d.attrs['zoom-step'] = zoom_step
    d.attrs['max-length'] = d.attrs['max-pos'] - d.attrs['min-pos'] + 1
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
    print("chroms to use:", chroms_to_use)

    print("min-pos:", d.attrs['min-pos'])
    print('max-pos:', d.attrs['max-pos'])

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
            counter += remaining
            curr_zoom = 0
            data_buffers[0] += values
            curr_time = time.time() - t1
            percent_progress = (positions[curr_zoom] + 1) / float(data_size)
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
             "Use all chromosomes if not set.")
@click.option(
        '--tile-size',
        '-t',
        default=1024,
        help="The number of data points in each tile."
             "Used to determine the number of zoom levels"
             "to create.")
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
def bigwig(filepath, output_file, assembly, chromosome, tile_size, chunk_size, zoom_step):
    _bigwig(filepath, chunk_size, zoom_step, tile_size, output_file, assembly, chromosome)

@aggregate.command()
@click.argument( 
        'filepath',
        metavar='FILEPATH')
@click.option(
        '--output-file',
        '-o',
        default=None,
        help="The default output file name to use. If this isn't"
             "specified, clodius will replace the current extension"
             "with .multires.bed"
        )
@click.option(
        '--assembly',
        '-a',
        help='The genome assembly that this file was created against',
        default='hg19')
@click.option(
        '--importance-column',
        help='The column (1-based) containing information about how important'
        "that row is. If it's absent, then use the length of the region."
        "If the value is equal to `random`, then a random value will be"
        "used for the importance (effectively leading to random sampling)"
        )
@click.option(
        '--chromosome',
        default=None,
        help="Only extract values for a particular chromosome."
             "Use all chromosomes if not set."
             )
@click.option(
        '--max-per-tile',
        default=100,
        type=int)
@click.option(
        '--tile-size', 
        default=1024,
        help="The number of nucleotides that the highest resolution tiles should span."
             "This determines the maximum zoom level"
        )
def bedfile(filepath, output_file, assembly, importance_column, chromosome, max_per_tile, tile_size):
    _bedfile(filepath, output_file, assembly, importance_column, chromosome, max_per_tile, tile_size)
