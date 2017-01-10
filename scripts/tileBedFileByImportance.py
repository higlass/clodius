#!/usr/bin/python

from __future__ import print_function

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
import sys
import argparse

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
    
    python tileBedFileByImportance.py file.bed
""")

    parser.add_argument('bedfile')
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

        genome_start = nc.chr_pos_to_genome_pos(str(line.chrom), line.start, args.assembly)
        genome_end = nc.chr_pos_to_genome_pos(line.chrom, line.stop, args.assembly)
        parts = [genome_start, genome_end] + map(str,line.fields[3:]) + [slugid.nice()] + [importance]

        return parts


    dset = sorted([line_to_np_array(line) for line in bed_file], key=lambda x: x[0])
    min_feature_width = min(map(lambda x: int(x[1]) - int(x[0]), dset))
    max_feature_width = max(map(lambda x: int(x[1]) - int(x[0]), dset))

    # The tileset will be stored as an hdf5 file
    '''
    print("Writing to: {}".format(args.output_file), file=sys.stderr)
    f = h5py.File(args.output_file, 'w')
    '''

    # We neeed chromosome information as well as the assembly size to properly
    # tile this data
    tile_size = args.tile_size
    chrom_info = nc.get_chrominfo(args.assembly)
    assembly_size = chrom_info.total_length
    #max_zoom = int(math.ceil(math.log(assembly_size / min_feature_width) / math.log(2)))
    max_zoom = int(math.ceil(math.log(assembly_size / tile_size) / math.log(2)))

    # store some meta data
    '''
    d = f.create_dataset('meta', (1,), dtype='f')

    d.attrs['zoom-step'] = 1            # we'll store data for every zoom level
    d.attrs['max-length'] = assembly_size
    d.attrs['assembly'] = args.assembly
    d.attrs['chrom-names'] = nc.get_chromorder(args.assembly)
    d.attrs['chrom-sizes'] = nc.get_chromsizes(args.assembly)
    d.attrs['chrom-order'] = nc.get_chromorder(args.assembly)
    d.attrs['tile-size'] = tile_size
    d.attrs['max-zoom'] = max_zoom
    d.attrs['max-width'] = tile_size * 2 ** max_zoom
    '''

    max_width = tile_size * 2 ** max_zoom
    interval_tree = itree.IntervalTree()
    uid_to_entry = {}

    intervals = []


    for d in dset:
        uid = d[-2]
        uid_to_entry[uid] = d
        intervals += [(d[0], d[1], uid)]
        #interval_tree[d[0]:d[1]] = uid

        #interval_tree.verify()

        #print("interval_tree[{}:{}]='{}'".format(d[0],d[1], uid))

    tile_width = tile_size

    # each entry in pdset will correspond to the values visible for that tile

    removed = set()

    curr_zoom = 0
    while curr_zoom <= max_zoom:
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


            #print("len:", len(values_in_tile), file=sys.stderr)

            if len(values_in_tile) > 0:
                for v in values_in_tile:
                    '''
                    if v[0] < from_value and v[1] > to_value:
                        print("from_value:", from_value, "to_value", to_value, "entry", v[0], v[1], v[2])
                    '''
                    print("{}\t{}".format(curr_zoom, "\t".join(map(str,uid_to_entry[v[-1]]))))
                    intervals.remove(v)
                    #print("interval_tree.remove(itree.Interval({},{},'{}'))".format(v[0], v[1], v[2]))
                    #interval_tree.verify()
                    #interval_tree.remove(v)
                    #removed.add(v[0])
        print ("curr_zoom:", curr_zoom, file=sys.stderr)
        curr_zoom += 1


    return

    # add a tile position
    '''
    pdset = (cf.ParallelData(dset).map(lambda x: [-1] + x)
            .flatMap(lambda x: spread_across_tiles(x, tile_size)))
    f.create_dataset('{}'.format(int(max_zoom)), 
                     data=sorted(pdset.data, key=lambda x: x[0]), compression='gzip')

    print("pdset:", len(pdset.data))
    '''
    
    #pdset = cf.ParallelData(dset).flatMap(lambda x: 

    curr_zoom = max_zoom - 1
    tiles = col.defaultdict(list)

    for item in dset:
        pass



    while curr_zoom >= 0:
        print ("pd3:", pdset.take(4))
        pdset = (pdset.map(lambda x: (x[-2], x))   # remove the tile anchor generated by
                 .reduceByKey(lambda e1, e2: e1))      # spread_across_tiles and order by uid
        
        print("pd2:", pdset.take(4))
        pdset_orig = cf.ParallelData([x[1] for x in pdset.collect()])
        tile_width = tile_size * 2 ** (max_zoom - curr_zoom)

        pdset_spread = pdset_orig.flatMap(lambda x: spread_across_tiles(x, tile_width))
        print("pd1:", pdset.take(2))
        print("tile_width:", tile_width)
        
        # calculate the tile number for each entry
        pdset = pdset_spread.map(lambda x: (int(x[0] / tile_width), [x]))
        pdset = pdset.reduceByKey(lambda e1,e2: reduce_values_by_importance(e1, e2, 
            max_entries_per_tile = args.max_per_tile))
        #tile_nums_values = [(int(d[0] / tile_size), d) for d in dset]
        pdset = pdset.map(lambda x: (x[0] / 2, x[1]))

        new_dset = [item for sublist in [d[1] for d in pdset.collect()] for item in sublist]
        print("curr_zoom:", curr_zoom, "new_dset:", len(new_dset))
        f.create_dataset('{}'.format(int(curr_zoom)), 
                data=sorted(new_dset, key=lambda x: x[0]), compression='gzip')

        pdset = cf.ParallelData(new_dset)
        curr_zoom -= 1

    #print("dset:", dset)
    

if __name__ == '__main__':
    main()


