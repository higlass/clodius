# -*- coding: utf-8 -*-
from __future__ import division, print_function

from . import cli

import click
import clodius.chromosomes as cch
import clodius.multivec as cmv
import clodius.array as ct
import collections as col
import h5py
import math
import negspy.coordinates as nc
import numpy as np
import os
import os.path as op
import random
import scipy.misc as sm
import slugid
import sqlite3
import sys
import time
import gzip
import json

from .utils import get_tile_pos_from_lng_lat


@cli.group()
def aggregate():
    '''
    Aggregate a data file so that it stores the data at multiple
    resolutions.
    '''
    pass


def store_meta_data(
    cursor,
    zoom_step,
    max_length,
    assembly,
    chrom_names,
    chrom_sizes,
    tile_size,
    max_zoom,
    max_width,
    header=[]
):
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
            max_width REAL,
            header text
        )
        ''')

    cursor.execute(
        'INSERT INTO tileset_info VALUES (?,?,?,?,?,?,?,?,?)', (
            zoom_step,
            max_length,
            assembly,
            "\t".join(chrom_names),
            "\t".join(map(str, chrom_sizes)),
            tile_size,
            max_zoom,
            max_width,
            "\t".join(header)
        )
    )

    cursor.commit()

    pass


# all entries are broked up into ((tile_pos), [entry]) tuples
# we just need to reduce the tiles so that no tile contains more than
# max_entries_per_tile entries
# (notice that [entry] is an array), this format will be important when
# reducing to the most important values
def reduce_values_by_importance(
    entry1, entry2, max_entries_per_tile=100, reverse_importance=False
):
    def extract_key(entries):
        return [(e[-2], e) for e in entries]
    by_uid = dict(extract_key(entry1) + extract_key(entry2))
    combined_by_uid = by_uid.values()

    if reverse_importance:
        combined_entries = sorted(
            combined_by_uid,
            key=lambda x: float(x[-1])
        )
    else:
        combined_entries = sorted(
            combined_by_uid,
            key=lambda x: -float(x[-1])
        )

    return combined_entries[:max_entries_per_tile]


def _multivec(filepath, output_file, assembly, tile_size, chromsizes_filename, starting_resolution, method, row_infos_filename=None):
    '''
    Aggregate a multivec file.

    This is a file containing nxn data that is aggregated along only one axis.
    This data should be in an HDF5 file where each dataset is named for a
    chromosome and contains a 'resolutions' group containing values for the
    base level resolution.

    Example: f['chr1']['reslutions']['1000'] = [[1,2,3],[4,5,6]]

    The resulting data will be organized by resolution and chromosome.

    Example: f_out['chr1']['resolutions']['5000']=[[1000,2000,3000],[4000,5000,6000]]

    Aggregation is currently done by summing adjacent values.
    '''
    f_in = h5py.File(filepath, 'r')

    if output_file is None:
        output_file = op.splitext(filepath)[0] + ".multires.mv5"

    (chrom_info, chrom_names, chrom_sizes) = cch.load_chromsizes(
        chromsizes_filename, assembly)

    # TODO: "method" is not defined, so this would not work?
    if method == 'maxtotal':  # noqa: F821
        pass
    if method == 'logsumexp':  # noqa: F821
        def agg(x):
            a = x.T.reshape((x.shape[1], -1, 2))
            return sm.logsumexp(a, axis=2).T
    else:
        def agg(x):
            return x.T.reshape((x.shape[1], -1, 2)).sum(axis=2).T

    print("agg:", agg)
    if row_infos_filename is not None:
        with open(row_infos_filename, 'r') as fr:
            row_infos = [l.strip().encode('utf8') for l in fr]
    else:
        row_infos = None
    print("row_infos:", row_infos)

    cmv.create_multivec_multires(f_in,
                                 chromsizes=zip(chrom_names, chrom_sizes),
                                 agg=lambda x: np.nansum(x.T.reshape(
                                     (x.shape[1], -1, 2)), axis=2).T,
                                 starting_resolution=starting_resolution,
                                 tile_size=tile_size,
                                 output_file=output_file,
                                 row_infos=row_infos)


def _bedpe(filepath, output_file,
           assembly, importance_column, has_header,
           max_per_tile, tile_size, chromosome=None,
           chromsizes_filename=None,
           chr1_col=1, from1_col=2, to1_col=3,
           chr2_col=4, from2_col=5, to2_col=6,
           max_zoom=None):

    print('output_file:', output_file)

    if filepath == '-':
        f = sys.stdin
    elif filepath.endswith('.gz'):
        f = gzip.open(filepath, 'rt')
    else:
        print("plain")
        f = open(filepath, 'r')

    if output_file is None:
        output_file = filepath + ".multires.db"
    else:
        output_file = output_file

    if op.exists(output_file):
        os.remove(output_file)

    (chrom_info, chrom_names, chrom_sizes) = cch.load_chromsizes(
        chromsizes_filename, assembly)

    def line_to_dict(line):
        parts = line.split()
        d = {}
        try:
            d['xs'] = [
                chrom_info.cum_chrom_lengths[
                    parts[chr1_col - 1]] + int(parts[from1_col - 1]),
                chrom_info.cum_chrom_lengths[
                    parts[chr1_col - 1]] + int(parts[to1_col - 1])
            ]
            d['ys'] = [
                chrom_info.cum_chrom_lengths[
                    parts[chr2_col - 1]] + int(parts[from2_col - 1]),
                chrom_info.cum_chrom_lengths[
                    parts[chr2_col - 1]] + int(parts[to2_col - 1])
            ]
        except KeyError:
            error_str = (
                "ERROR converting chromosome position to genome position. "
                "Please make sure you've specified the correct assembly "
                "using the --assembly option or a chromsizes file using the . "
                "--chromsizes-filename option."
                "Current assembly: {}, chromosomes: {},{}".format(
                    assembly,
                    parts[chr1_col - 1], parts[chr2_col - 1]
                )
            )
            raise(KeyError(error_str))

        d['uid'] = slugid.nice()

        d['chrOffset'] = d['xs'][0] - int(parts[from1_col - 1])

        if importance_column is None:
            d['importance'] = max(
                d['xs'][1] - d['xs'][0], d['ys'][1] - d['ys'][0]
            )
        elif importance_column == 'random':
            d['importance'] = random.random()
        else:
            # We seem to use one-based numbering for columns...
            d['importance'] = float(parts[int(importance_column) - 1])

        d['fields'] = line

        return d

    entries = []

    if has_header:
        f.readline()
    else:
        first_line = f.readline().strip()
        try:
            parts = first_line.split()

            '''
            print("chr1_col", chr1_col, "chr2_col", chr2_col,
                  "from1_col:", from1_col, "from2_col", from2_col,
                  "to1_col", to1_col, "to2_col", to2_col)
            '''

            int(parts[from1_col - 1])
            int(parts[to1_col - 1])
            int(parts[from2_col - 1])
            int(parts[to2_col - 1])
        except ValueError:
            error_str = (
                "Couldn't convert one of the bedpe coordinates to an "
                "integer. If the input file contains a header, make sure to "
                "indicate that with the --has-header option. Line: {}"
                .format(first_line)
            )
            raise ValueError(error_str)
        entries = [line_to_dict(first_line)]

    entries += [line_to_dict(line)
                for line in [line.strip() for line in f] if line]

    # We neeed chromosome information as well as the assembly size to properly
    # tile this data
    tile_size = tile_size
    assembly_size = chrom_info.total_length + 1
    max_zoom = int(
        math.ceil(math.log(assembly_size / tile_size) / math.log(2))
    )
    '''
    if max_zoom is not None and max_zoom < max_zoom:
        max_zoom = max_zoom
    '''

    # this script stores data in a sqlite database
    sqlite3.register_adapter(np.int64, lambda val: int(val))
    conn = sqlite3.connect(output_file)

    # store some meta data
    store_meta_data(
        conn, 1,
        max_length=assembly_size,
        assembly=assembly,
        chrom_names=chrom_names,
        chrom_sizes=chrom_sizes,
        tile_size=tile_size,
        max_zoom=max_zoom,
        max_width=tile_size * 2 ** max_zoom
    )

    # max_width = tile_size * 2 ** max_zoom
    # uid_to_entry = {}

    c = conn.cursor()
    c.execute(
        '''
        CREATE TABLE intervals
        (
            id int PRIMARY KEY,
            zoomLevel int,
            importance real,
            fromX int,
            toX int,
            fromY int,
            toY int,
            chrOffset int,
            uid text,
            fields text
        )
        '''
    )

    c.execute('''
        CREATE VIRTUAL TABLE position_index USING rtree(
            id,
            rFromX, rToX,
            rFromY, rToY
        )
        ''')

    curr_zoom = 0
    counter = 0

    tile_counts = col.defaultdict(
        lambda: col.defaultdict(lambda: col.defaultdict(int))
    )
    entries = sorted(entries, key=lambda x: -x['importance'])

    counter = 0
    for d in entries:
        curr_zoom = 0

        while curr_zoom <= max_zoom:
            tile_width = tile_size * 2 ** (max_zoom - curr_zoom)
            tile_from = list(
                map(lambda x: x / tile_width, [d['xs'][0], d['ys'][0]])
            )
            tile_to = list(
                map(lambda x: x / tile_width, [d['xs'][1], d['ys'][1]])
            )

            empty_tiles = True

            # go through and check if any of the tiles at this zoom level are
            # full

            for i in range(int(tile_from[0]), int(tile_to[0]) + 1):
                if not empty_tiles:
                    break

                for j in range(int(tile_from[1]), int(tile_to[1]) + 1):
                    if tile_counts[curr_zoom][i][j] > max_per_tile:

                        empty_tiles = False
                        break

            if empty_tiles:
                # they're all empty so add this interval to this zoom level
                for i in range(int(tile_from[0]), int(tile_to[0]) + 1):
                    for j in range(int(tile_from[1]), int(tile_to[1]) + 1):
                        tile_counts[curr_zoom][i][j] += 1

                c.execute(
                    'INSERT INTO intervals VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (
                        counter,
                        curr_zoom,
                        d['importance'],
                        d['xs'][0], d['xs'][1],
                        d['ys'][0], d['ys'][1],
                        d['chrOffset'],
                        d['uid'],
                        d['fields']
                    )
                )
                conn.commit()

                c.execute(
                    'INSERT INTO position_index VALUES (?,?,?,?,?)',
                    (
                        counter, d['xs'][0], d['xs'][1],
                        d['ys'][0], d['ys'][1]
                    )  # add counter as a primary key
                )
                conn.commit()

                counter += 1
                break

            curr_zoom += 1

    return


def _bedfile(
    filepath,
    output_file,
    assembly,
    importance_column,
    has_header,
    chromosome,
    max_per_tile,
    tile_size,
    delimiter,
    chromsizes_filename,
    offset
):
    if output_file is None:
        output_file = filepath + ".beddb"
    else:
        output_file = output_file

    if op.exists(output_file):
        os.remove(output_file)

    if filepath.endswith('.gz'):
        import gzip
        bed_file = gzip.open(filepath, 'rt')
    else:
        bed_file = open(filepath, 'r')

    try:
        (chrom_info, chrom_names, chrom_sizes) = cch.load_chromsizes(chromsizes_filename, assembly)
    except FileNotFoundError:
        if chromsizes_filename is None:
            print("Assembly not found:", assembly, file=sys.stderr)
        else:
            print("Chromsizes filename not found:", chromsizes_filename, file=sys.stderr)
        return None

    rand = random.Random(3)

    def line_to_np_array(line):
        '''
        Convert a bed file line to a numpy array which can later
        be used as an entry in an h5py file.
        '''
        try:
            start = int(line[1])
            stop = int(line[2])
        except ValueError:
            raise ValueError(
                "Error parsing the position, line: {}".format(line)
            )

        chrom = line[0]

        if importance_column is None:
            # assume a random importance when no aggregation strategy is given
            importance = rand.random()
        elif importance_column == 'size':
            importance = stop - start
        elif importance_column == 'random':
            importance = rand.random()
        else:
            importance = float(line[int(importance_column) - 1])

        if stop < start:
            print("WARNING: stop < start:", line, file=sys.stderr)

            start, stop = stop, start

        # convert chromosome coordinates to genome coordinates
        genome_start = chrom_info.cum_chrom_lengths[chrom] + start + offset
        genome_end = chrom_info.cum_chrom_lengths[chrom] + stop + offset

        pos_offset = genome_start - start
        parts = {
            'startPos': genome_start,
            'endPos': genome_end,
            'uid': slugid.nice(),
            'chrOffset': pos_offset,
            'fields': '\t'.join(line),
            'importance': importance,
            'chromosome': str(chrom)
        }

        return parts

    dset = []

    print("delimiter:", delimiter)
    if has_header:
        line = bed_file.readline()
        header = line.strip().split(delimiter)
    else:
        line = bed_file.readline().strip()
        line_parts = line.strip().split(delimiter)
        try:
            dset += [line_to_np_array(line_parts)]
        except KeyError:
            print(f'Unable to find {line_parts[0]} in the list of chromosome sizes. '
                  'Please make sure the correct assembly or chromsizes filename '
                  'is passed in as a parameter', file=sys.stderr)
            return None
        except IndexError:
            print("Invalid line:", line)
        header = map(
            str, list(range(1, len(line.strip().split(delimiter)) + 1)))

    for line in bed_file:
        line_parts = line.strip().split(delimiter)
        try:
            dset += [line_to_np_array(line_parts)]
        except IndexError:
            print("Invalid line:", line)

    if chromosome is not None:
        dset = [d for d in dset if d['chromosome'] == chromosome]

    # We neeed chromosome information as well as the assembly size to properly
    # tile this data
    tile_size = tile_size

    assembly_size = chrom_info.total_length + 1
    '''
    else:
        try:
            assembly_size = chrom_info.chrom_lengths[chromosome]
        except KeyError:
            print(
                "ERROR: Chromosome {} not found in assembly {}.".format(
                    chromosome, assembly
                ),
                file=sys.stderr
            )
            return 1
    '''

    max_zoom = int(
        math.ceil(math.log(assembly_size / tile_size) / math.log(2))
    )
    '''
    if max_zoom is not None and max_zoom < max_zoom:
        max_zoom = max_zoom
    '''

    # this script stores data in a sqlite database
    import sqlite3
    sqlite3.register_adapter(np.int64, lambda val: int(val))
    print("output_file:", output_file)
    conn = sqlite3.connect(output_file)

    # store some meta data
    store_meta_data(
        conn,
        1,
        max_length=assembly_size,
        assembly=assembly,
        chrom_names=chrom_names,
        chrom_sizes=chrom_sizes,
        tile_size=tile_size,
        max_zoom=max_zoom,
        max_width=tile_size * 2 ** max_zoom,
        header=header,
    )

    # max_width = tile_size * 2 ** max_zoom
    uid_to_entry = {}

    intervals = []

    # store each bed file entry as an interval
    for d in dset:
        uid = d['uid']
        uid_to_entry[uid] = d
        intervals += [(d['startPos'], d['endPos'], uid)]

    tile_width = tile_size

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
        '''
    )

    c.execute(
        '''
        CREATE VIRTUAL TABLE position_index USING rtree(
            id,
            rStartPos, rEndPos
        )
        '''
    )

    curr_zoom = 0
    counter = 0

    max_viewable_zoom = max_zoom

    if max_zoom is not None and max_zoom < max_zoom:
        max_viewable_zoom = max_zoom

    sorted_intervals = sorted(intervals,
                              key=lambda x: -uid_to_entry[x[-1]]['importance'])
    # print('si:', sorted_intervals[:10])
    print("max_per_tile:", max_per_tile)

    tile_counts = col.defaultdict(int)

    for interval in sorted_intervals:
        # go through each interval from most important to least
        while curr_zoom <= max_viewable_zoom:
            # try to place it in the highest zoom level and go down from there
            tile_width = tile_size * 2 ** (max_zoom - curr_zoom)

            curr_pos = interval[0]
            space_available = True

            # check if there's space at this zoom level
            while curr_pos < interval[1]:
                curr_tile = math.floor(curr_pos / tile_width)
                tile_id = '{}.{}'.format(curr_zoom, curr_tile)

                '''
                if interval[0] < 1000000:
                    print('tile_id:', tile_id, tile_counts[tile_id], curr_zoom, 'interval:', interval)
                '''

                # print(tile_id, "tile_counts[tile_id]", tile_counts[tile_id])
                if tile_counts[tile_id] >= max_per_tile:
                    space_available = False
                    break

                curr_pos += tile_width

            # if there is, then fill it up
            if space_available:
                curr_pos = interval[0]
                while curr_pos < interval[1]:
                    curr_tile = math.floor(curr_pos / tile_width)
                    tile_id = '{}.{}'.format(curr_zoom, curr_tile)

                    tile_counts[tile_id] += 1

                    '''
                    # increment tile counts for lower level tiles
                    higher_zoom = curr_zoom + 1
                    higher_tile = math.floor(higher_zoom / 2)

                    while higher_zoom <= max_viewable_zoom:
                        new_tile_id = '{}.{}'.format(higher_zoom, higher_tile)
                        higher_zoom += 1
                        higher_tile = math.floor(higher_tile / 2)
                        tile_counts[new_tile_id] += 1
                    '''

                    curr_pos += tile_width

            if space_available:
                # there's available space
                value = uid_to_entry[interval[-1]]

                # one extra question mark for the primary key
                exec_statement = 'INSERT INTO intervals VALUES (?,?,?,?,?,?,?,?)'

                c.execute(
                    exec_statement,
                    # primary key, zoomLevel, startPos, endPos, chrOffset, line
                    (counter, curr_zoom,
                     value['importance'],
                     value['startPos'], value['endPos'],
                     value['chrOffset'],
                     value['uid'],
                     value['fields'])
                )

                if counter % 1000 == 0:
                    print('counter:', counter,
                          value['endPos'] - value['startPos'])

                exec_statement = 'INSERT INTO position_index VALUES (?,?,?)'
                c.execute(
                    exec_statement,
                    # add counter as a primary key
                    (counter, value['startPos'], value['endPos'])
                )

                counter += 1
                break

            curr_zoom += 1

        curr_zoom = 0
    conn.commit()
    return True

###############################################################################


def _bedgraph(
    filepath,
    output_file,
    assembly,
    chrom_col,
    from_pos_col,
    to_pos_col,
    value_col,
    has_header,
    chromosome,
    tile_size,
    chunk_size,
    method,
    nan_value,
    transform,
    count_nan,
    closed_interval,
    chromsizes_filename,
    zoom_step
):
    if output_file is None:
        output_file = op.splitext(filepath)[0] + '.hitile'

    print("output file:", output_file)

    # Override the output file if it existts
    if op.exists(output_file):
        os.remove(output_file)
    f = h5py.File(output_file, 'w')

    # get the information about the chromosomes in this assembly
    if chromsizes_filename is not None:
        chrom_info = nc.get_chrominfo_from_file(chromsizes_filename)
        chrom_order = [
            a.encode('utf-8')
            for a
            in nc.get_chromorder_from_file(chromsizes_filename)
        ]
        chrom_sizes = nc.get_chromsizes_from_file(chromsizes_filename)
    else:
        chrom_info = nc.get_chrominfo(assembly)
        chrom_order = [a.encode('utf-8') for a in nc.get_chromorder(assembly)]
        chrom_sizes = nc.get_chromsizes(assembly)

    assembly_size = chrom_info.total_length
    print('assembly_size:', assembly_size)

    tile_size = tile_size
    # how many values to read in at once while tiling
    chunk_size = tile_size * 2 ** chunk_size

    dsets = []  # data sets at each zoom level
    nan_dsets = []  # store nan values

    # initialize the arrays which will store the values at each stored zoom
    # level
    z = 0
    positions = []   # store where we are at the current dataset
    data_buffers = [[]]
    nan_data_buffers = [[]]

    while assembly_size / 2 ** z > tile_size:
        dset_length = math.ceil(assembly_size / 2 ** z)
        dsets += [
            f.create_dataset(
                'values_' + str(z),
                (dset_length,),
                dtype='f',
                compression='gzip'
            )
        ]
        nan_dsets += [
            f.create_dataset(
                'nan_values_' + str(z),
                (dset_length,),
                dtype='f',
                compression='gzip'
            )
        ]

        data_buffers += [[]]
        nan_data_buffers += [[]]

        positions += [0]
        z += zoom_step

    # store some meta data
    d = f.create_dataset('meta', (1,), dtype='f')

    print("assembly:", assembly)

    d.attrs['zoom-step'] = zoom_step
    d.attrs['max-length'] = assembly_size
    d.attrs['assembly'] = assembly
    d.attrs['chrom-names'] = chrom_order
    d.attrs['chrom-sizes'] = chrom_sizes
    d.attrs['chrom-order'] = chrom_order
    d.attrs['tile-size'] = tile_size
    d.attrs['max-zoom'] = max_zoom = math.ceil(
        math.log(d.attrs['max-length'] / tile_size) / math.log(2)
    )
    d.attrs['max-width'] = tile_size * 2 ** max_zoom
    d.attrs['max-position'] = 0

    print("assembly size (max-length)", d.attrs['max-length'])
    print("max-width", d.attrs['max-width'])
    print("max_zoom:", d.attrs['max-zoom'])
    print("chunk-size:", chunk_size)
    print("chrom-order", d.attrs['chrom-order'])

    t1 = time.time()

    # are we reading the input from stdin or from a file?

    if filepath == '-':
        f = sys.stdin
    else:
        if filepath.endswith('.gz'):
            import gzip
            f = gzip.open(filepath, 'rt')
        else:
            f = open(filepath, 'r')

    curr_zoom = 0

    def add_values_to_data_buffers(buffers_to_add, nan_buffers_to_add):
        curr_zoom = 0

        data_buffers[0] += buffers_to_add
        nan_data_buffers[0] += nan_buffers_to_add

        curr_time = time.time() - t1
        percent_progress = (positions[curr_zoom] + 1) / float(assembly_size)
        print(
            "position: {} progress: {:.2f} elapsed: {:.2f} "
            "remaining: {:.2f}".format(
                positions[curr_zoom] + 1,
                percent_progress,
                curr_time, curr_time / (percent_progress) - curr_time
            )
        )

        while len(data_buffers[curr_zoom]) >= chunk_size:
            # get the current chunk and store it, converting nans to 0
            print("len(data_buffers[curr_zoom])", len(data_buffers[curr_zoom]))
            curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
            nan_curr_chunk = np.array(nan_data_buffers[curr_zoom][:chunk_size])

            '''
            print("1cc:", sum(curr_chunk))
            print("1db:", data_buffers[curr_zoom][:chunk_size])
            print("1curr_chunk:", nan_curr_chunk)
            '''
            print("positions[curr_zoom]:", positions[curr_zoom])

            curr_pos = positions[curr_zoom]
            dsets[curr_zoom][curr_pos:curr_pos + chunk_size] = curr_chunk
            nan_dsets[curr_zoom][curr_pos:curr_pos +
                                 chunk_size] = nan_curr_chunk

            # aggregate and store aggregated values in the next zoom_level's
            # data
            data_buffers[curr_zoom + 1] += list(
                ct.aggregate(curr_chunk, 2 ** zoom_step)
            )
            nan_data_buffers[curr_zoom + 1] += list(
                ct.aggregate(nan_curr_chunk, 2 ** zoom_step)
            )

            data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
            nan_data_buffers[curr_zoom] =\
                nan_data_buffers[curr_zoom][chunk_size:]

            # data = data_buffers[curr_zoom+1]
            # nan_data = nan_data_buffers[curr_zoom+1]

            # do the same for the nan values buffers

            positions[curr_zoom] += chunk_size
            curr_zoom += 1

            if curr_zoom * zoom_step >= max_zoom:
                break

    values = []
    nan_values = []

    if has_header:
        f.readline()

    # the genome position up to which we've filled in values
    curr_genome_pos = 0

    # keep track of the previous value so that we can use it to fill in NAN
    # values
    # prev_value = 0

    for line in f:
        # each line should indicate a chromsome, start position and end
        # position
        parts = line.strip().split()

        start_genome_pos = (
            chrom_info.cum_chrom_lengths[parts[chrom_col - 1]] +
            int(parts[from_pos_col - 1])
        )

        if start_genome_pos - curr_genome_pos > 1:
            values += [np.nan] * (start_genome_pos - curr_genome_pos - 1)
            nan_values += [1] * (start_genome_pos - curr_genome_pos - 1)

            curr_genome_pos += (start_genome_pos - curr_genome_pos - 1)

        # count how many nan values there are in the dataset
        nan_count = 1 if parts[value_col - 1] == nan_value else 0

        # if the provided values are log2 transformed, we have to un-transform
        # them
        if transform == 'exp2':
            value = (
                2 ** float(parts[value_col - 1])
                if not parts[value_col - 1] == nan_value
                else np.nan
            )
        else:
            value = (
                float(parts[value_col - 1])
                if not parts[value_col - 1] == nan_value
                else np.nan
            )

        # we're going to add as many values are as specified in the bedfile line
        values_to_add = [
            value] * (int(parts[to_pos_col - 1]) - int(parts[from_pos_col - 1]))
        nan_counts_to_add = [
            nan_count] * (int(parts[to_pos_col - 1]) - int(parts[from_pos_col - 1]))

        if closed_interval:
            values_to_add += [value]
            nan_counts_to_add += [nan_count]

        # print("values_to_add", values_to_add)

        values += values_to_add
        nan_values += nan_counts_to_add

        d.attrs['max-position'] = start_genome_pos + len(values_to_add)

        curr_genome_pos += len(values_to_add)

        while len(values) > chunk_size:
            print("len(values):", len(values), chunk_size)
            print("line:", line)
            add_values_to_data_buffers(
                values[:chunk_size], nan_values[:chunk_size]
            )
            values = values[chunk_size:]
            nan_values = nan_values[chunk_size:]

    add_values_to_data_buffers(values, nan_values)

    # store the remaining data
    while True:
        # get the current chunk and store it
        chunk_size = len(data_buffers[curr_zoom])
        curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
        nan_curr_chunk = np.array(nan_data_buffers[curr_zoom][:chunk_size])

        '''
        print("2curr_chunk", curr_chunk)
        print("2curr_zoom:", curr_zoom)
        print("2db", data_buffers[curr_zoom][:100])
        '''

        curr_pos = positions[curr_zoom]
        dsets[curr_zoom][curr_pos:curr_pos + chunk_size] = curr_chunk
        nan_dsets[curr_zoom][curr_pos:curr_pos + chunk_size] = nan_curr_chunk

        # aggregate and store aggregated values in the next zoom_level's data
        data_buffers[curr_zoom + 1] += list(
            ct.aggregate(curr_chunk, 2 ** zoom_step)
        )
        nan_data_buffers[curr_zoom + 1] += list(
            ct.aggregate(nan_curr_chunk, 2 ** zoom_step)
        )

        data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
        nan_data_buffers[curr_zoom] = nan_data_buffers[curr_zoom][chunk_size:]

        # data = data_buffers[curr_zoom+1]
        # nan_data = nan_data_buffers[curr_zoom+1]

        positions[curr_zoom] += chunk_size
        curr_zoom += 1

        # we've created enough tile levels to cover the entire maximum width
        if curr_zoom * zoom_step >= max_zoom:
            break

    # still need to take care of the last chunk


def _geojson(filepath, output_file, max_per_tile, tile_size, max_zoom):
    if filepath == '-':
        f = sys.stdin
    elif filepath.endswith('.gz'):
        f = gzip.open(filepath, 'rt')
    else:
        f = open(filepath, 'r')

    if output_file is None:
        output_file = filepath + ".gjdb"
    else:
        output_file = output_file

    if op.exists(output_file):
        os.remove(output_file)

    geojson = json.load(f)

    entries = []

    def getRect(coords, no_area_comp):
        minX = math.inf
        maxX = -math.inf
        minY = math.inf
        maxY = -math.inf
        area = 0.0  # Calculated with the shoelace formula
        n = len(coords)

        try:
            for coord_group in coords:
                for i, coord in enumerate(coord_group):
                    minX = min(minX, coord[0])
                    maxX = max(maxX, coord[0])
                    minY = min(minY, coord[1])
                    maxY = max(maxY, coord[1])
                    if not no_area_comp:
                        j = (i + 1) % n
                        area += coord_group[i][0] * coord_group[j][1]
                        area -= coord_group[j][0] * coord_group[i][1]

                area = abs(area) / 2.0
        except TypeError:
            # coords aren't iterable so this must be a point
            minX = coords[0]
            maxX = coords[0]
            minY = coords[1]
            maxY = coords[1]

            # points don't have an area so let's just pick something
            area = random.random()

        return minX, maxX, minY, maxY, abs(area) / 2.0

    for feature in geojson['features']:
        try:
            area = feature['properties']['area']
        except:
            area = None
            pass

        try:
            uid = feature['properties']['uid']
        except:
            uid = None
            pass

        try:
            minLng, maxLng, minLat, maxLat, _area = getRect(
                feature['geometry']['coordinates'], area
            )
            entries.append({
                'minLng': minLng,
                'maxLng': maxLng,
                'minLat': minLat,
                'maxLat': maxLat,
                'importance': area or _area,
                'uid': uid or slugid.nice(),
                'geometry': json.dumps(feature['geometry']),
                'properties': json.dumps(feature['properties']),
            })
        except Exception:
            raise

    # this script stores data in a sqlite database
    sqlite3.register_adapter(np.int64, lambda val: int(val))
    conn = sqlite3.connect(output_file)

    # store some meta data
    conn.execute('''
        CREATE TABLE tileset_info
        (
            zoom_step INT,
            tile_size INT,
            max_zoom INT,
            min_x INT,
            max_x INT,
            min_y INT,
            max_y INT
        )
        ''')

    conn.execute(
        'INSERT INTO tileset_info VALUES (?,?,?,?,?,?,?)', (
            1,
            tile_size,
            19,
            -180,
            180,
            -90,
            90
        )
    )
    conn.commit()

    # max_width = tile_size * 2 ** max_zoom
    # uid_to_entry = {}

    c = conn.cursor()
    c.execute(
        '''
        CREATE TABLE intervals
        (
            id int PRIMARY KEY,
            zoomLevel int,
            importance real,
            minLng int,
            maxLng int,
            minLat int,
            maxLat int,
            uid text,
            geometry text,
            properties text
        )
        '''
    )

    c.execute('''
        CREATE VIRTUAL TABLE position_index USING rtree(
            id,
            rMinLng, rMaxLng,
            rMinLat, rMaxLat
        )
        ''')

    curr_zoom = 0
    counter = 0

    tile_counts = col.defaultdict(
        lambda: col.defaultdict(lambda: col.defaultdict(int))
    )
    entries = sorted(entries, key=lambda x: -x['importance'])

    counter = 0
    for d in entries:
        curr_zoom = 0

        while curr_zoom <= max_zoom:
            tile_from = get_tile_pos_from_lng_lat(
                d['minLng'], d['maxLat'], curr_zoom
            )

            tile_to = get_tile_pos_from_lng_lat(
                d['maxLng'], d['minLat'], curr_zoom
            )

            empty_tiles = True

            # go through and check if any of the tiles at this zoom level are
            # full
            for i in range(int(tile_from[0]), int(tile_to[0]) + 1):
                if not empty_tiles:
                    break

                for j in range(int(tile_from[1]), int(tile_to[1]) + 1):
                    if tile_counts[curr_zoom][i][j] > max_per_tile:

                        empty_tiles = False
                        break

            if empty_tiles:
                # they're all empty so add this interval to this zoom level
                for i in range(int(tile_from[0]), int(tile_to[0]) + 1):
                    for j in range(int(tile_from[1]), int(tile_to[1]) + 1):
                        tile_counts[curr_zoom][i][j] += 1

                c.execute(
                    'INSERT INTO intervals VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (
                        counter,
                        curr_zoom,
                        d['importance'],
                        d['minLng'], d['maxLng'],
                        d['minLat'], d['maxLat'],
                        d['uid'],
                        d['geometry'],
                        d['properties'],
                    )
                )
                conn.commit()

                c.execute(
                    'INSERT INTO position_index VALUES (?,?,?,?,?)',
                    (
                        # add counter as a primary key
                        counter,
                        d['minLng'], d['maxLng'],
                        d['minLat'], d['maxLat'],
                    )
                )
                conn.commit()

                counter += 1
                break

            curr_zoom += 1

    f.close()


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
    type=click.Choice(nc.available_chromsizes()),
    default='hg19'
)
@click.option(
    '--chromosome',
    default=None,
    help="Only extract values for a particular chromosome."
         "Use all chromosomes if not set."
)
@click.option(
    '--tile-size',
    '-t',
    default=1024,
    help="The number of data points in each tile."
         "Used to determine the number of zoom levels"
         "to create."
)
@click.option(
    '--chunk-size',
    '-c',
    help='How many values to aggregate at once.'
         'Specified as a power of two multiplier of the tile'
         'size',
    default=14
)
@click.option(
    '--chromosome-col',
    help="The column number (1-based) which contains the chromosome "
         "name",
    default=1
)
@click.option(
    '--from-pos-col',
    help="The column number (1-based) which contains the starting "
         "position",
    default=2
)
@click.option(
    '--to-pos-col',
    help="The column number (1-based) which contains the ending"
         "position",
    default=3
)
@click.option(
    '--value-col',
    help="The column number (1-based) which contains the actual value"
         "position",
    default=4
)
@click.option(
    '--has-header/--no-header',
    help="Does this file have a header that we should ignore",
    default=False
)
@click.option(
    '--method',
    help='The method used to aggregate values (e.g. sum, average...)',
    type=click.Choice(['sum', 'average']),
    default='sum'
)
@click.option(
    '--nan-value',
    help='The string to use as a NaN value',
    type=str,
    default=None
)
@click.option(
    '--transform',
    help='The method used to aggregate values (e.g. sum, average...)',
    type=click.Choice(['none', 'exp2']),
    default='none'
)
@click.option(
    '--count-nan',
    help="Simply count the number of nan values in the file",
    is_flag=True
)
@click.option(
    '--closed-interval',
    help="Treat the to column as a closed interval",
    is_flag=True)
@click.option(
    '--chromsizes-filename',
    help="A file containing chromosome sizes and order",
    default=None)
@click.option(
    '--zoom-step',
    '-z',
    help="The number of intermediate aggregation levels to"
         "omit",
    default=8)
def bedgraph(
        filepath, output_file, assembly, chromosome_col,
        from_pos_col, to_pos_col, value_col, has_header,
        chromosome, tile_size, chunk_size, method, nan_value,
        transform, count_nan, closed_interval,
        chromsizes_filename, zoom_step):
    _bedgraph(
        filepath, output_file, assembly, chromosome_col,
        from_pos_col, to_pos_col, value_col, has_header,
        chromosome, tile_size, chunk_size, method, nan_value,
        transform, count_nan, closed_interval,
        chromsizes_filename, zoom_step)


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
         "with .multires.bed"
)
@click.option(
    '--assembly',
    '-a',
    help='The genome assembly that this file was created against',
    default='hg19'
)
@click.option(
    '--importance-column',
    help='The column (1-based) containing information about how important'
    "that row is. If it's absent, then use the length of the region."
    "If the value is equal to `random`, then a random value will be"
    "used for the importance (effectively leading to random sampling)"
)
@click.option(
    '--has-header/--no-header',
    help="Does this file have a header that we should ignore",
    default=False
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
    type=int,
    help="The maximum number of entries to store per tile")
@click.option(
    '--tile-size',
    default=1024,
    help="The number of nucleotides that the highest resolution tiles "
         "should span. This determines the maximum zoom level"
)
@click.option(
    '--delimiter',
    default=None,
    type=str
)
@click.option(
    '--chromsizes-filename',
    help="A file containing chromosome sizes and order",
    default=None
)
@click.option(
    '--offset',
    help="Apply an offset to all the coordinates in this file",
    type=int,
    default=0
)
def bedfile(
    filepath, output_file, assembly, importance_column, has_header,
    chromosome, max_per_tile, tile_size, delimiter, chromsizes_filename,
    offset
):
    _bedfile(
        filepath, output_file, assembly, importance_column, has_header,
        chromosome, max_per_tile, tile_size, delimiter, chromsizes_filename,
        offset
    )


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
         "with .bed2db"
)
@click.option(
    '--assembly',
    '-a',
    help='The genome assembly that this file was created against',
    default='hg19'
)
@click.option(
    '--importance-column',
    help='The column (1-based) containing information about how important'
    "that row is. If it's absent, then use the length of the region."
    "If the value is equal to `random`, then a random value will be"
    "used for the importance (effectively leading to random sampling)",
    default='random'
)
@click.option(
    '--has-header/--no-header',
    help="Does this file have a header that we should ignore",
    default=False
)
@click.option(
    '--max-per-tile',
    default=100,
    type=int,
    help="The maximum number of entries to include per tile"
)
@click.option(
    '--tile-size',
    default=1024,
    help="The number of nucleotides that the highest resolution tiles "
         "should span. This determines the maximum zoom level"
)
@click.option(
    '--chromosome',
    default=None,
    help="Only extract values for a particular chromosome."
         "Use all chromosomes if not set."
)
@click.option(
    '--chromsizes-filename',
    help="A file containing chromosome sizes and order",
    default=None
)
@click.option(
    '--chr1-col',
    default=1,
    help="The column containing the first chromosome"
)
@click.option(
    '--chr2-col',
    default=4,
    help="The column containing the second chromosome"
)
@click.option(
    '--from1-col',
    default=2,
    help="The column containing the first start position"
)
@click.option(
    '--from2-col',
    default=5,
    help="The column containing the second start position"
)
@click.option(
    '--to1-col',
    default=3,
    help="The column containing the first end position"
)
@click.option(
    '--to2-col',
    default=6,
    help="The column containing the second end position"
)
def bedpe(
        filepath, output_file, assembly, importance_column,
        has_header, max_per_tile, tile_size, chromosome,
        chromsizes_filename,
        chr1_col, from1_col, to1_col,
        chr2_col, from2_col, to2_col
):
    """Aggregate bedpe files"""
    _bedpe(
        filepath, output_file,
        assembly, importance_column, has_header,
        max_per_tile, tile_size, chromosome,
        chromsizes_filename,
        chr1_col=chr1_col, from1_col=from1_col, to1_col=to1_col,
        chr2_col=chr2_col, from2_col=from2_col, to2_col=to2_col
    )


@aggregate.command()
@click.argument(
    'filepath',
    metavar='FILEPATH'
)
@click.option(
    '-o',
    '--output-file',
    default=None,
    help="The default output file name to use. If this isn't"
         "specified, clodius will replace the current extension"
         "with .gjdb"
)
@click.option(
    '-a',
    '--assembly',
    default=None,
    help="The assembly that this data comes from. This parameter is"
         "unnecessary and/or overwritten if --chromsizes-filename is specified")
@click.option(
    '-s',
    '--tile-size',
    default=256,
    help="The number of nucleotides that the highest resolution tiles "
         "should span. This determines the maximum zoom level"
)
@click.option(
    '-c',
    '--chromsizes-filename',
    default=None,
    help="The file containnig chromosome sizes and order")
@click.option(
    '--starting-resolution',
    default=256,
    help="The resolution that the starting data is at (e.g. 1, 10, 20)")
@click.option(
    '--method',
    help='The method used to aggregate values (e.g. sum, average...)',
    type=click.Choice(['sum', 'logsumexp']),
    default='sum')
@click.option(
    '--row-infos-filename',
    help="A file containing the names of the rows in the multivec file",
    default=None)
def multivec(
        filepath, output_file, assembly, tile_size,
        chromsizes_filename, starting_resolution, method,
        row_infos_filename):
    """Aggregate a multivec file"""
    _multivec(
        filepath, output_file, assembly, tile_size,
        chromsizes_filename, starting_resolution, method,
        row_infos_filename)


@aggregate.command()
@click.argument(
    'filepath',
    metavar='FILEPATH'
)
@click.option(
    '-o',
    '--output-file',
    default=None,
    help="The default output file name to use. If this isn't"
         "specified, clodius will replace the current extension"
         "with .gjdb"
)
@click.option(
    '-m',
    '--max-per-tile',
    default=20,
    type=int
)
@click.option(
    '-s',
    '--tile-size',
    default=256,
    help="The number of nucleotides that the highest resolution tiles "
         "should span. This determines the maximum zoom level"
)
@click.option(
    '-z',
    '--max-zoom',
    default=19,
    help="The number of nucleotides that the highest resolution tiles "
         "should span. This determines the maximum zoom level"
)
def geojson(
    filepath, output_file, max_per_tile, tile_size, max_zoom
):
    """Aggregate a geojson file"""
    _geojson(
        filepath, output_file, max_per_tile, tile_size, max_zoom
    )
