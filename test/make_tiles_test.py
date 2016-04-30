import shortuuid
import sys

sys.path.append("scripts")

import make_tiles as mt

def test_make_matrix_tiles():
    '''
    Test the creation of matrix tiles. Example input matrix:

    pos1 pos2 count
    2	3   1
    3	5   1
    5   6   1
    6   2   1
    7   9   1
    8   7   1
    9   4   1
    10  5   1

    In this case

    '''

    class Options:
        def __init__(self, value, importance, position,
                column_names = None):
            self.importance = importance
            self.value = value
            self.position = position
            self.max_entries_per_tile = 1
            self.max_zoom = 3
            self.column_names = column_names
            self.min_value = None
            self.max_value = None
            self.resolutions = "1,1"

    options = Options('count', None, 'pos1,pos2')

    #mt.make_tiles_from_file('test/data/simpleMatrix.tsv', options)

    pass

def test_make_tiles_by_index():
    entries = mt.load_entries_from_file('test/data/simpleMatrix.tsv', 
                column_names = ['pos1', 'pos2', 'count'])
    dim_names = ['pos1', 'pos2']
    
    max_zoom = 2

    print "entries:", entries
    tiles = mt.make_tiles_by_index(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2)

    '''
    for key,value in tiles['tiles'].items():
        print "tile:", key, value['shown']
    '''

def test_make_tiles_with_resolution():
    entries = mt.load_entries_from_file('test/data/smallFullMatrix.tsv', 
                column_names = ['pos1', 'pos2', 'count'])

    dim_names = ['pos1', 'pos2']
    max_zoom = 1
    # create sparse format tiles (default)
    tiles = mt.make_tiles_by_index(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2,
            resolution=1)

    tiles = tiles['tiles'].collect()
    print "tiles:", tiles

    for key,value in tiles:
        print "tile:", key, value

    # make sure the top-level tile is there
    assert((0,0,0) in [t[0] for t in tiles])
    assert('count' in tiles[0][1][0])

    # create dense format tiles
    tiles = mt.make_tiles_by_index(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2,
            output_format = 'dense',
            resolution=1)
    tiles = tiles['tiles'].collect()

    for key, value in tiles:
        print "tile:", key, value

    assert(type(tiles[0][1][0] == float))
