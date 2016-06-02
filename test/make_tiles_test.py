import shortuuid
import sys
import clodius.fpark as cfp
import clodius.tiles as cmt

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

    #mt.make_tiles_from_file('test/sample_data/simpleMatrix.tsv', options)

    pass

def test_make_tiles_by_binning():
    entries = cmt.load_entries_from_file('test/sample_data/simpleMatrix.tsv', 
                column_names = ['pos1', 'pos2', 'count'])
    dim_names = ['pos1', 'pos2']
    
    max_zoom = 2

    tiles = cmt.make_tiles_by_binning(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2)

def test_make_tiles_with_resolution():
    entries = cmt.load_entries_from_file('test/sample_data/smallFullMatrix.tsv', 
                column_names = ['pos1', 'pos2', 'count'])

    dim_names = ['pos1', 'pos2']
    max_zoom = 1
    # create sparse format tiles (default)
    tiles = cmt.make_tiles_by_binning(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2,
            resolution=1)

    print "tiles:", tiles
    tiles = tiles['tiles'].collect()

    print "tiles:", tiles

    # make sure the top-level tile is there
    assert((0,0,0) in [t[0] for t in tiles])
    assert('dense' in tiles[0][1])

    # create dense format tiles
    tiles = cmt.make_tiles_by_binning(entries, dim_names, max_zoom,
            value_field='count',
            bins_per_dimension=2,
            resolution=1)
    tiles = tiles['tiles'].collect()

def test_make_tiles_with_importance():
    entries = cmt.load_entries_from_file('test/sample_data/smallRefGeneCounts.tsv',
            column_names=['refseqid', 'chr', 'strand', 'txStart', 'txEnd', 'genomeTxStart', 'genomeTxEnd', 'cdsStart', 'cdsEnd', 'exonCount', 'exonStarts', 'exonEnds', 'count'])

    #tiles = cmt.make_tiles_by_importance(entries, dim_names, max_zoom, value_field
    dim_names = ['txStart']
    max_zoom = None

    tiles = cmt.make_tiles_by_importance(entries, dim_names = ['txStart'], 
            max_zoom = None, 
            importance_field='count', 
            max_entries_per_tile=1)

    for (tile_pos, tile_values) in tiles['tiles'].collect():
        assert(len(tile_values) <= 1)

def test_data_bounds():
    entries = cmt.load_entries_from_file('test/sample_data/smallBedGraph.tsv', 
            column_names=['chr1', 'pos1', 'pos2', 'val'],
            delimiter=' ')

    dim_names = ['pos1']
    entries.map(cmt.add_pos(dim_names))

    (mins, maxs) = cmt.data_bounds(entries, 1)

    assert(mins[0] == 1.0)
    assert(maxs[0] == 8.0)

def test_position_ranges():
    entries = cmt.load_entries_from_file('test/sample_data/smallBedGraph.tsv', 
            column_names=['chr1', 'pos1', 'pos2', 'val'],
            delimiter=' ')
    entries = entries.map(lambda x: dict(x, pos1=int(x['pos1']), pos2=int(x['pos2'])))

    entries = entries.flatMap(lambda x: cmt.expand_range(x, 'pos1', 'pos2'))
    froms = entries.map(lambda x: x['pos1']).collect()

    assert(1 in froms)
    assert(2 in froms)
    assert(3 in froms)
    assert(4 in froms)
    assert(8 in froms)
    assert(9 in froms)

    for entry in entries.collect():
        assert(entry['pos1'] != 5)
        assert(entry['pos1'] != 10)

def test_dnase_sample_data():
    entries = cmt.load_entries_from_file('test/sample_data/E116-DNase.fc.signal.bigwig.bedGraph.genome.225',
            column_names=['pos1', 'pos2', 'val'], delimiter=None)
    entries = entries.flatMap(lambda x: cmt.expand_range(x, 'pos1', 'pos2', range_except_0 = 'val'))

    tile_sample_data = cmt.make_tiles_by_binning(entries, 
            ['pos1'], max_zoom = 1000,
            value_field = 'val', importance_field = 'val',
            resolution = 1, bins_per_dimension = 64)

    tile = tile_sample_data['tiles'].collect()[0]

def test_end_position():
    sc = cfp.FakeSparkContext

    # make one really wide entry
    entries = [{'x1': 1, 'x2': 10, 'value': 5}]
    entries = cfp.FakeSparkContext.parallelize(entries)

    tileset = cmt.make_tiles_by_importance(sc, entries, ['x1'], end_dim_names=['x2'], max_zoom=2, 
                                           importance_field='value', adapt_zoom=False)

    tiles = tileset['tiles'].collect()
    tile_ids = map(lambda x: x[0], tiles)

    print "tiles:", tiles, tile_ids

    # this data point should be in every tile
    assert((0,0) in tile_ids)
    assert((1,1) in tile_ids)
    assert((1,0) in tile_ids)
    assert((2,0) in tile_ids)
    assert((2,3) in tile_ids)

    # make two not-so-wide entries
    entries = [{'x1': 1, 'value': 5}, {'x1': 10, 'value': 6}]
    entries = cfp.FakeSparkContext.parallelize(entries)

    tileset = cmt.make_tiles_by_importance(sc, entries, ['x1'], max_zoom=2, 
                                           importance_field='value', adapt_zoom=False)

    tiles = tileset['tiles'].collect()
    print "tiles:", tiles
    tile_ids = map(lambda x: x[0], tiles)

    print "tiles:", tiles, tile_ids

    # this data point should be in every tile
    assert((0,0) in tile_ids)
    assert((1,0) in tile_ids)
    assert((1,2) in tile_ids)

