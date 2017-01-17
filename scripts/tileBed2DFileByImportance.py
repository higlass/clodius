#!/usr/bin/python

from __future__ import print_function

import argparse
import clodius.fpark as cf
import collections as col
import gzip
import h5py
import math
import negspy.coordinates as nc
import numpy as np
import slugid
import os
import os.path as op
import random
import sqlite3
import sys

def store_meta_data(cursor, zoom_step, max_length, assembly, chrom_names, 
        chrom_sizes, tile_size, max_zoom, max_width):
    print("chrom_names:", chrom_names)

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
    parser.add_argument('outfile', default=None, nargs='?')
    parser.add_argument('--importance-column', type=str,
            help='The column (1-based) containing information about how important'
            "that row is. If it's absent, then use the length of the region."
            "If the value is equal to `random`, then a random value will be"
            "used for the importance (effectively leading to random sampling)")
    parser.add_argument('--assembly', type=str, default='hg19',
            help='The genome assembly to use')
    parser.add_argument('--skip-first-line', default=False, action="store_true",
            help="Skip the first line (likely because it's a comment)")
    parser.add_argument('--max-per-tile', type=int, default=100)
    parser.add_argument('--tile-size', default=1024)
    parser.add_argument('-o', '--output-file')
    parser.add_argument('--max-zoom', type=int, help="The default maximum zoom value")

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')

    args = parser.parse_args()

    if args.bedfile[-3:] == '.gz':
        print("gzip")
        f = gzip.open(args.bedfile, 'r')
    else:
        print("plain")
        f = open(args.bedfile, 'r')

    if args.outfile is None:
        outfile = args.bedfile + ".multires.db"
    else:
        outfile = args.outfile

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
        parts = {
                    'startPos': genome_start,
                    'endPos': genome_end,
                    'uid': slugid.nice(),
                    'chrOffset': pos_offset,
                    'fields': '\t'.join(line.fields),
                    'importance': importance
                    }

        return parts

    def line_to_dict(line):
        parts = line.split()
        d = {}
        d['froms'] = [nc.chr_pos_to_genome_pos(parts[0], int(parts[1]), args.assembly), 
                      nc.chr_pos_to_genome_pos(parts[0], int(parts[2]), args.assembly)]
        d['tos'] = [nc.chr_pos_to_genome_pos(parts[3], int(parts[4]), args.assembly), 
                    nc.chr_pos_to_genome_pos(parts[3], int(parts[5]), args.assembly)]

        d['pos_offset'] = d['froms'][0] - int(parts[1])

        if args.importance_column is None:
            d['importance'] = max(d['tos'][1] - d['froms'][1], d['tos'][0] - d['froms'][0]) 
        elif args.importance_column == 'random':
            d['importance'] = random.random()
        else:
            d['importance'] = float(d[args.importance_column])

        d['line'] = line

    if args.skip_first_line:
        f.readline()
    dset = [line_to_dict(line) for line in f]

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
    conn = sqlite3.connect(outfile)

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
        fromX int,
        toX int,
        fromY int,
        toY int,
        chrOffset int,
        fields text
    )
    ''')

    print("creating rtree")
    c.execute('''
        CREATE VIRTUAL TABLE position_index USING rtree(
            id,
            fromX, toX,
            fromY, toY
        )
        ''')

    curr_zoom = 0
    counter = 0
    
    max_viewable_zoom = max_zoom

    if args.max_zoom is not None and args.max_zoom < max_zoom:
        max_viewable_zoom = args.max_zoom


    tile_counts = col.defaultdict(lambda: col.defaultdict(lambda: col.defaultdict(int)))
    
    counter = 0
    for d in entries:
        curr_zoom = 0

        while curr_zoom >= max_zoom:
            tile_from = map(d['froms'], lambda x: x / tile_width)
            tile_to = map(d['tos'], lambda x: x / tile_width)

            empty_tiles = True

            # go through and check if any of the tiles at this zoom level are full
            for i in range(tile_from[0], tile_to[0]+1):
                if not empty_tiles:
                    break

                for j in range(tile_from[1], tile_to[1]+1):
                    if tile_counts[curr_zoom][i][j] > args.max_per_tile:
                        empty_tiles = False
                        break

            
            if empty_tiles:
                # they're all empty so add this interval to this zoom level
                for i in range(tile_from[0], tile_to[0]+1):
                    for j in range(tile_from[1], tile_to[1]+1):
                        tile_counts[curr_zoom][i][j] += 1

                exec_statement = 'INSERT INTO intervals VALUES (?,?,?,?,?,?,?,?,?)'
                ret = c.execute(
                        exec_statement,
                        (counter, curr_zoom, 
                            value['importance'],
                            value['startPos'], value['endPos'],
                            value['froms'][0], value['tos'][0],
                            value['froms'][1], values['tos'][1],
                            value['chrOffset'], value['fields'])
                        )
                conn.commit()

                exec_statement = 'INSERT INTO position_index VALUES (?,?,?,?,?)'
                ret = c.execute(
                        exec_statement,
                        (counter, value['froms'][0], value['tos'][0], 
                            value['froms'][1], value['tos'][1])  #add counter as a primary key
                        )
                conn.commit()

                counter += 1

            
                        


            curr_zoom += 1


    

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
                    exec_statement = 'INSERT INTO intervals VALUES (?,?,?,?,?,?,?)'
                    ret = c.execute(
                            exec_statement,
                            # primary key, zoomLevel, startPos, endPos, chrOffset, line
                            (counter, curr_zoom, 
                                value['importance'],
                                value['startPos'], value['endPos'],
                                value['chrOffset'], value['fields'])
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


