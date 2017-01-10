#!/usr/bin/python

from __future__ import print_function

import argparse
import clodius.fpark as cf
import collections as col
import intervaltree as itree
import h5py
import math
import negspy.coordinates as nc
import numpy as np
import slugid
import os
import os.path as op
import pybedtools as pbt
import random
import sqlite3
import sys

def store_meta_data(conn, zoom_step, max_length, assembly, chrom_names, 
        chrom_sizes, chrom_order, tile_size, max_zoom, max_width):
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

    #print("ce", [c[-1] for c in combined_entries[:max_entries_per_tile]])
    #print("ce", [c[2] - c[1] for c in combined_entries[:max_entries_per_tile]])
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
    parser.add_argument('--assembly', type=str, default='hg19',
            help='The genome assembly to use')
    parser.add_argument('--max-per-tile', type=int, default=100)
    parser.add_argument('--tile-size', default=1024)
    parser.add_argument('-o', '--output-file', default='/tmp/tmp.hdf5')
    parser.add_argument('--max-zoom', type=int, help="The default maximum zoom value")

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')

    args = parser.parse_args()

    if op.exists(args.output_file):
        os.remove(args.output_file)

    bed_file = pbt.BedTool(args.bedfile)

    def line_to_np_array(line):
        '''
        Convert a bed file line to a numpy array which can later
        be used as an entry in an h5py file.
        '''

        if args.importance_column is None:
            importance = line.stop - line.start
        elif args.importance_column == 'random':
            imporance = random.random()
        else:
            importance = int(line.fields[int(args.importance_column)-1])

        # convert chromosome coordinates to genome coordinates
        genome_start = nc.chr_pos_to_genome_pos(str(line.chrom), line.start, args.assembly)
        genome_end = nc.chr_pos_to_genome_pos(line.chrom, line.stop, args.assembly)
        pos_offset = genome_start - line.start
        parts = [genome_start, genome_end] + map(str,line.fields[3:]) + [slugid.nice()] + [importance] + [pos_offset]

        return parts

    dset = sorted([line_to_np_array(line) for line in bed_file], key=lambda x: x[0])
    min_feature_width = min(map(lambda x: int(x[1]) - int(x[0]), dset))
    max_feature_width = max(map(lambda x: int(x[1]) - int(x[0]), dset))


    # We neeed chromosome information as well as the assembly size to properly
    # tile this data
    tile_size = args.tile_size
    chrom_info = nc.get_chrominfo(args.assembly)
    assembly_size = chrom_info.total_length
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
            chrom_order = nc.get_chromorder(args.assembly),
            tile_size = tile_size,
            max_zoom = max_zoom,
            max_width = tile_size * 2 ** max_zoom)

    max_width = tile_size * 2 ** max_zoom
    interval_tree = itree.IntervalTree()
    uid_to_entry = {}

    intervals = []

    # store each bed file entry as an interval
    for d in dset:
        uid = d[-3]
        uid_to_entry[uid] = d
        intervals += [(d[0], d[1], uid)]

    tile_width = tile_size

    removed = set()

    c = conn.cursor()
    c.execute(
    '''
    CREATE TABLE intervals
    (
        id int,
        zoomLevel int,
        startPos int,
        endPos int,
        geneName text,
        score real,
        strand text,
        transcriptId text,
        geneId int,
        geneType text,
        geneDesc text,
        cdsStart int,
        cdsEnd int,
        exonStarts text,
        exonEnds text,
        uid TEXT PRIMARY KEY,
        importance real,
        pos_offset int
    )
    ''')

    print("creating rtree")
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
                    key=lambda x: -uid_to_entry[x[-1]][-1])[:args.max_per_tile]   # the importance is always the last column
                                                            # take the negative because we want to prioritize
                                                            # higher values


            if len(values_in_tile) > 0:
                for v in values_in_tile:
                    '''
                    if v[0] < from_value and v[1] > to_value:
                        print("from_value:", from_value, "to_value", to_value, "entry", v[0], v[1], v[2])
                    '''
                    counter += 1
                    output_line = "{}\t{}".format(curr_zoom, "\t".join(map(str,uid_to_entry[v[-1]])))
                    output_parts = output_line.split('\t')

                    # one extra question mark for the primary key
                    exec_statement = 'INSERT INTO intervals VALUES (?,{})'.format(",".join(['?' for p in output_parts])) 
                    ret = c.execute(
                            exec_statement,
                            tuple([counter] + output_parts)     # add counter as a primary key
                            )
                    conn.commit()

                    exec_statement = 'INSERT INTO position_index VALUES (?,?,?)'
                    ret = c.execute(
                            exec_statement,
                            tuple([counter] + [output_parts[1], output_parts[2]])     # add counter as a primary key
                            )
                    conn.commit()
                    intervals.remove(v)
        print ("curr_zoom:", curr_zoom, file=sys.stderr)
        curr_zoom += 1

    conn.commit()

    conn.close()


    return
    

if __name__ == '__main__':
    main()


