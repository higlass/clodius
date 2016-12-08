#!/usr/bin/python

from __future__ import print_function

import clodius.hdf_tiles as hdft
import h5py
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="""

    python get_hitile.py filename z x
""")

    parser.add_argument('filename')
    parser.add_argument('z', type=int)
    parser.add_argument('x', type=int)
    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')

    args = parser.parse_args()

    with h5py.File(args.filename, 'r') as f:
        tileset_info = hdft.get_tileset_info(f)
        max_width = tileset_info['max_width']
        max_pos = tileset_info['max_pos']
        tile_size = tileset_info['tile_size']

        print("max_width", max_width)
        print("max_pos", max_pos)

        last_index = int(tile_size * (max_pos / max_width))
        print("last_index:", last_index)
        tile_data = hdft.get_data(f, args.z, args.x)

        #print("tile:", hdft.get_data(f, args.z, args.x))

     
if __name__ == '__main__':
    main()

