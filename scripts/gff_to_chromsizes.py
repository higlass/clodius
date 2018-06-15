#!/usr/bin/python

import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="""
    
    python gff_to_chromsizes.py
""")

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #                     help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #                     help='Another useless option')

    args = parser.parse_args()
    

if __name__ == '__main__':
    main()


