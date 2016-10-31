#!/usr/bin/python

import numpy as np
import pandas as pd
import weave
import sys
import time
import argparse

def tile2(in_array):
    length = len(in_array)
    out_array = np.zeros(length / 2)
    print "length:", length
    print "new_length", length / 2
    support = "#include <math.h>"

    code = """
    for (int i = 0; i < length-1; i += 2) {
        out_array[i / 2] = in_array[i] + in_array[i+1];
    }
    """
    weave.inline(code, ['in_array', 'out_array', 'length'], 
            support_code = support,
            libraries=['m'])

    return out_array

def main():
    parser = argparse.ArgumentParser(description="""
    
    python main.py
""")

    parser.add_argument('-f', '--filepath', default=None)
    parser.add_argument('-c', '--chunk-size', default=2**14)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')
    args = parser.parse_args()
    last_end = 0
    data = []

    if args.filepath is None:
        print "Waiting for input..."
        for line in sys.stdin:
            parts = line.split()
            start = int(parts[0], 10)
            end = int(parts[1], 10)
            val = float(parts[2])

            if start > last_end:
                # in case there's skipped values in the bed file
                data += [0] * (last_end - start)
            
            data += [float(parts[2])] * (end - start)
            last_end = end
    else:
        data = pd.read_csv(args.filepath, sep=' ', header=None)

    data = np.array(data)
    tile_size = 256
    t1 = time.time()
    while len(data) > tile_size:
        new_data = tile2(data)
        data = new_data


    print "time:", time.time() - t1
    print "len(data):", len(data)

    #print data

if __name__ == '__main__':
    main()


