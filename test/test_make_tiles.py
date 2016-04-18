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

def test_filter_entries():
    entries = mt.load_entries_from_file('test/data/simpleMatrix.tsv')
    filter_interval = ((2, 6), (2, 5))
    dim_names = ['pos1', 'pos2']

    filtered_data = mt.filter_data(entries, dim_names,
            min_bounds = [x[0] for x in filter_interval],
            max_bounds = [x[1] for x in filter_interval])

    assert(len(filtered_data) == 1)  # should only contain {"pos1": 2, "pos2": 3, "count": 1}

    filter_interval = ((2, 1), (2, 5))
    filtered_data = mt.filter_data(entries, dim_names,
            min_bounds = [x[0] for x in filter_interval],
            max_bounds = [x[1] for x in filter_interval])

    assert(len(filtered_data) == 0)

def test_make_tiles_by_index():
    entries = mt.load_entries_from_file('test/data/simpleMatrix.tsv')
    dim_names = ['pos1', 'pos2']
    
    max_zoom = 2

    mt.make_tiles_by_index(entries, dim_names, max_zoom)

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

