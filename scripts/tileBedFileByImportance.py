#!/usr/bin/python

from __future__ import print_function

import negspy.coordinates as nc
import pybedtools as pbt
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="""
    
    python tileBedFileByImportance.py file.bed
""")

    parser.add_argument('bedfile')
    parser.add_argument('--importance-column', type=float, 
            help='The column containing information about how important'
            "that row is. If it's absent, then use the length of the region")
    parser.add_argument('--assembly', type=str, default='hg19',
            help='The genome assembly to use')
    parser.add_argument('--max-per-tile', type=int, default=100)

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')

    args = parser.parse_args()

    bed_file = pbt.BedTool(args.bedfile)

    for line in bed_file:
        if args.importance_column is None:
            importance = line.stop - line.start
        else:
            importance = line.fields[importance]

        genome_start = nc.chr_pos_to_genome_pos(str(line.chrom), line.start, args.assembly)
        genome_end = nc.chr_pos_to_genome_pos(line.chrom, line.stop, args.assembly)
        print("genome_start:", genome_start, "genome_end:", genome_end, "importance:", importance)

    

if __name__ == '__main__':
    main()


