#!/usr/bin/python

from __future__ import print_function

import argparse
import math
import negspy.coordinates as nc
import slugid
import os
import os.path as op
import pybedtools as pbt
import random
import sqlite3
import sys

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

def main():
    parser = argparse.ArgumentParser(description="""

    python tileBedFileByImportance.py file.bed output.db

    Tile a bed file and store it as a sqlite database
""")

    parser.add_argument('bedfile')
    parser.add_argument('outfile')
    parser.add_argument('--importance-column', type=str,
            help='The column (1-based) containing information about how important'
            "that row is. If it's absent, then use the length of the region."
            "If the value is equal to `random`, then a random value will be"
            "used for the importance (effectively leading to random sampling)")
    parser.add_argument('-a', '--assembly', type=str, default='hg19',
            help='The genome assembly to use')
    parser.add_argument('--max-per-tile', type=int, default=100)
    parser.add_argument('--tile-size', default=1024)
    parser.add_argument('-o', '--output-file', default='/tmp/tmp.hdf5')
    parser.add_argument('--max-zoom', type=int, help="The default maximum zoom value")
    parser.add_argument('--chromosome', help="Tile only values for a particular chromosome")

    args = parser.parse_args()

    if op.exists(args.outfile):
        os.remove(args.outfile)

    bed_file = pbt.BedTool(args.bedfile)

    def line_to_np_array(line):
        '''
        Convert a bed file line to a numpy array which can later
        be used as an entry in an h5py file.
        '''

        if args.importance_column is None:
            importance = line.stop - line.start
        elif args.importance_column == 'random':
            importance = random.random()
        else:
            importance = int(line.fields[int(args.importance_column)-1])

        # convert chromosome coordinates to genome coordinates

        if args.chromosome is None:
            genome_start = nc.chr_pos_to_genome_pos(str(line.chrom), line.start, args.assembly)
            genome_end = nc.chr_pos_to_genome_pos(line.chrom, line.stop, args.assembly)
        else:
            genome_start = line.start
            genome_end = line.end

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
    
    if args.chromosome is not None:
        dset = [d for d in dset if d['chromosome'] == args.chromosome]

    # We neeed chromosome information as well as the assembly size to properly
    # tile this data
    tile_size = args.tile_size
    chrom_info = nc.get_chrominfo(args.assembly)
    if args.chromosome is None:
        assembly_size = chrom_info.total_length
    else:
        try:
            assembly_size = chrom_info.chrom_lengths[args.chromosome]
        except KeyError:
            print("ERROR: Chromosome {} not found in assembly {}.".format(args.chromosome, args.assembly), file=sys.stderr)
            return 1
    print("assembly-size:", assembly_size)

    #max_zoom = int(math.ceil(math.log(assembly_size / min_feature_width) / math.log(2)))
    max_zoom = int(math.ceil(math.log(assembly_size / tile_size) / math.log(2)))
    '''
    if args.max_zoom is not None and args.max_zoom < max_zoom:
        max_zoom = args.max_zoom
    '''

    # this script stores data in a sqlite database
    conn = sqlite3.connect(args.outfile)

    # store some meta data
    store_meta_data(conn, 1,
            max_length = assembly_size,
            assembly = args.assembly,
            chrom_names = nc.get_chromorder(args.assembly),
            chrom_sizes = nc.get_chromsizes(args.assembly),
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

    if args.max_zoom is not None and args.max_zoom < max_zoom:
        max_viewable_zoom = args.max_zoom

    while curr_zoom <= max_viewable_zoom and len(intervals) > 0:
        # at each zoom level, add the top genes
        tile_width = tile_size * 2 ** (max_zoom - curr_zoom)

        for tile_num in range(max_width / tile_width):
            # go over each tile and distribute the remaining values
            #values = interval_tree[tile_num * tile_width: (tile_num+1) * tile_width]
            from_value = tile_num * tile_width
            to_value = (tile_num + 1) * tile_width
            entries = [i for i in intervals if (i[0] < to_value and i[1] > from_value)]
            values_in_tile = sorted(entries,
                    key=lambda x: -uid_to_entry[x[-1]]['importance'])[:args.max_per_tile]   # the importance is always the last column
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


if __name__ == '__main__':
    main()


