#!/usr/bin/python

import Bio.SeqIO as bsio
import argparse
import gzip
import sys

def main():
    parser = argparse.ArgumentParser(description="""
    
    python create_chrominfo.py [genome.fa/-]

    Create a chrominfo file for a genome
""")

    #parser.add_argument('argument', nargs=1)
    parser.add_argument('fasta_file')

    args = parser.parse_args()

    if args.fasta_file == '-':
        f = sys.stdin
    elif args.fasta_file[-3:] == ".gz":
        f = gzip.open(args.fasta_file, 'r')
    else:
        f = open(args.fasta_file, 'rb')

    #fseq = bsio.parse(f, 'fasta')

    curr_name = None
    curr_size = 0

    for line in f:
        line = line.decode('utf-8')
        if line[0] == '>':
            if curr_name is not None:
                # print out the previous sequence
                print("{}\t{}".format(curr_name, curr_size))
                curr_size = 0
            curr_name = line.strip()[1:]
        else:
            curr_size += len(line.strip())
    print("{}\t{}".format(curr_name, curr_size))

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')
    '''
    for record in fseq:
        #print("record.id", record.id, len(record.seq), file=sys.stderr)
        print("{}\t{}".format(record.id, len(record.seq)))

    args = parser.parse_args()
    '''

    

if __name__ == '__main__':
    main()


