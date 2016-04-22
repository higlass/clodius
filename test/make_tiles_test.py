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
    tiles = mt.make_tiles_by_index(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2,
            resolution=1)

    for key,value in tiles['tiles'].items():
        print "tile:", key, value['shown']
        '''
        if key == (1,1,1):
            assert(abs(value['shown'][0]['pos'][0] - 3.0) < 0.0001)
            assert(abs(value['shown'][0]['pos'][1] - 3.0) < 0.0001)
        '''

def test_aggregate_tile_by_binning():
    '''
    Aggregate data into bins
    '''
    entries = mt.load_entries_from_file('test/data/simpleMatrix.tsv',
                column_names = ['pos1', 'pos2', 'count'])
    dim_names = ['pos1', 'pos2']

    # if the resolution is provided, we could go from the bottom up
    # and create zoom_widths that are multiples of the resolutions
    def consolidate_positions(entry):
        '''
        Place all of the dimensions in one array for this entry.
        '''
        value_field = 'count'
        importance_field = 'count'

        new_entry = {'pos': map(lambda dn: float(entry[dn]), dim_names),
                      value_field: float(entry[value_field]),
                      importance_field: float(entry[importance_field]),
                      'uid': shortuuid.uuid() }
        return new_entry

    entries = entries.map(consolidate_positions)
    print "entries:", entries

    tile = {'shown': entries,
            'tile_start_pos': [2, 2],
            'tile_end_pos': [10.01, 10.01]}

    new_tile = mt.aggregate_tile_by_binning(tile, bins_per_dimension=2)

    assert(len(new_tile['shown']) < 4)
    assert(len(new_tile['shown']) > 1)

    for data_point in new_tile['shown']:
        assert(data_point['count'] in [2,2,4])


'''
def test_make_tiles_recursively():
    entries = mt.load_entries_from_file('test/data/simpleMatrix.tsv')
    dim_names = ['pos1', 'pos2']

    (mins, maxs) = mt.data_bounds(entries, dim_names)

    tiles = mt.make_tiles_recursively(entries, dim_names, 
            zoom_level = 0, 
            max_zoom = 2, 
            mins = mins, maxs=maxs,
            resolutions = [1,1],
            value_field = 'count')

    print "tiles:", tiles
'''

