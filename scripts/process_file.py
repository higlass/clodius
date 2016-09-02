from __future__ import print_function

import argparse
import os.path as op
import subprocess as sp
import sys

def main():
    usage = """
    python make_tiles.py input_file

    Create tiles for all of the entries in the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('filepath')
    parser.add_argument('--assembly', default='hg19')
    parser.add_argument('--type', default='bedgraph')

    args = parser.parse_args()

    filedir = op.dirname(args.filepath)

    if args.type == 'bedgraph':
        outfile = open(args.filepath + '.genome.sorted.gz', 'w')
        p0 = sp.Popen(['pv', '-f', args.filepath], 
                        stdout=sp.PIPE, 
                        stderr=sp.PIPE, 
                        universal_newlines=True)
        p1 = sp.Popen(["awk", "{print $1, $2, $1, $3, $4 }"],  
                        stdin = p0.stdout,
                       stdout=sp.PIPE)
        p2 = sp.Popen(['chr_pos_to_genome_pos.py', '-e 5', '-a', '{}'.format(args.assembly)],
                        stdin = p1.stdout,
                        stdout=sp.PIPE)
        p3 = sp.Popen(['sort', '-k1,1n', '-k2,2n', '-'],
                        stdin = p2.stdout, 
                        stdout=sp.PIPE)
        p35 = sp.Popen(['pv', '-f', '-'],
                        stdin = p3.stdout,
                        stdout = sp.PIPE,
                        stderr = sp.PIPE,
                        universal_newlines=True)
        p4 = sp.Popen(['gzip'],
                        stdin = p35.stdout, stdout=outfile)

        for line in iter(p0.stderr.readline, ""):
            print("line:", line.strip())

        for line in iter(p35.stderr.readline, ""):
            print("line:", line.strip())

        p1.wait()
        p2.wait()
        p3.wait()
        p35.wait()
        p4.wait()
    else:
        print("Unknown file type", file=sys.stderr)

    print("filedir:", filedir)

if __name__ == '__main__':
    main()
