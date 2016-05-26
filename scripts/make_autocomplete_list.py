#!/usr/bin/python

import collections as col
import json
import os
import os.path as op
import sys
import argparse

def save_autcomplete_list(substr_dict, output_dir):
    '''
    Save the substring dictionary as a bunch of json files in
    output_dir
    '''
    if not op.exists(output_dir):
        os.makedirs(output_dir)

    for substr in substr_dict:
        # the 'ac' in the filename stands for 'autocomplete' and it's
        # added so that we can have a suggestions without for length 0
        # substrings (i.e. the when the user hasn't typed anything in yet)
        out_filename = op.join(output_dir, 'ac_' + substr + '.json')
        print >>sys.stderr, "substr:", substr, "out_filename:", out_filename
        with open(out_filename, 'w') as f:
            json.dump(substr_dict[substr], f)

def make_autocomplete_list(entries, options):
    '''
    Make a list of autocomplete suggestions for a list of json objects

    :param entries: A list of json objects
    :param options: A set of options (i.e. output_dir, etc...) indicating how
                    the tiles should be created
    :return: A list of tiles
    '''
    # group each entry in json_file according to its starting letter
    ngrams = []
    longest_name = max(map(len, entries))

    substrs = col.defaultdict(list)

    for entry in entries:
        if options.name not in entry:
            # if an entry doesn't have a name field, print a warning and continue
            #print >>sys.stderr, "Found entry without a name:", entry['uid']
            continue
        # for each entry get each substring and add the entry to the list
        # of entries containing that substring
        # these lists will then be pruned down to create autocomplete suggestions
        for size in range(0,len(entry[options.name])+1):
            for i in range(len(entry[options.name])-size+1):
                substr = entry[options.name][i:i+size]

                # make the substrings file and token friendly
                substr = substr.replace('/', ' ').lower()
                substr = ' '.join(substr.split()).replace(' ', '_')
                print >>sys.stderr, "substr:", i, size, substr

                substrs[substr] += [entry]

    for key in substrs:
        if options.reverse_importance:
            substrs[key] = sorted(substrs[key], key=lambda x: -float(x[options.importance]))[:options.max_entries_per_autocomplete]
        else:
            substrs[key] = sorted(substrs[key], key=lambda x: float(x[options.importance]))[:options.max_entries_per_autocomplete]

    return substrs
    # recurse further down

def main():
    parser = argparse.ArgumentParser(description="""
    
    python make_autocomplete_list.py processed-ski-area-elevations.json

    Create jsons for searching for ski areas. These will consist
    of all the n-grams found in the ski area names.
""")

    parser.add_argument('json_file', nargs=1)
    parser.add_argument('-n', '--name', default='name',
            help='The field in the json entry which specifies its name')
    parser.add_argument('-i', '--importance', default='importance',
            help='The field in the json entry which specifies how important \
                  it is (more important entries are displayed higher up in \
                  the autocomplete suggestions')
    parser.add_argument('-m', '--max-entries-per-autocomplete', default=10,
            help='The maximum number of entries to be displayed in the \
                  autocomplete suggestions')
    parser.add_argument('-r', '--reverse-importance', default=False,
            action='store_true',
            help='Use the reverse sorting of the importance value to gauge \
                  the worth of individual entries')
    parser.add_argument('-o', '--output-dir', 
            help="The directory into which to place the output files",
            required=True)

    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')

    args = parser.parse_args()

    with open(args.json_file[0], 'r') as f:
        json_file = json.load(f)

    tiles = make_autocomplete_list(json_file, args)
    save_autcomplete_list(tiles, args.output_dir)

if __name__ == '__main__':
    main()


