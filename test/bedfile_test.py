from __future__ import print_function

import click.testing as clt
import clodius.cli.aggregate as cca
import clodius.db_tiles as cdt
import os
import os.path as op
import sqlite3
import tempfile

testdir = op.realpath(op.dirname(__file__))


def test_nonstandard_chrom():
    filename = 'test/sample_data/test_non_standard_chrom.bed'
    f = tempfile.NamedTemporaryFile(delete=False)

    ret = cca._bedfile(filename, f.name,
                       'hg19', None, False,
                       None, 100, 1024, None, None, 0)

    assert ret is None

    ret = cca._bedfile(filename, f.name,
                       'dfsdfs', None, False,
                       None, 100, 1024, None, None, 0)

    assert ret is None


def test_get_tileset_info():
    filename = 'test/sample_data/gene_annotations.short.db'
    t = cdt.get_tileset_info(filename)

    assert(t['zoom_step'] == 1)
    assert(t['max_length'] == 3137161264)
    assert(t['max_width'] > 4000000000)
    assert(t['max_width'] < 5000000000)


def test_table_created():
    check_table('test/sample_data/gene_annotations.short.db')


def check_table(filename):
    conn = sqlite3.connect(filename)
    c = conn.cursor()

    print("fetching...")

    '''
    for row in c.execute('SELECT * from intervals'):
        print ("row:", row)
    '''

    rows = c.execute(
        'SELECT * from intervals,position_index '
        'where intervals.id=position_index.id '
        'and zoomLevel < 1 '
        'and rStartPos > 2400000000 '
        'and rEndPos < 2500000000')
    counter = 0
    for row in rows:
        assert(row[3] > 2400000000)
        assert(row[4] < 2500000000)
        counter += 1

    assert(counter > 0)


def test_get_tiles():
    filename = 'test/sample_data/gene_annotations.short.db'

    cdt.get_tiles(filename, 18, 169283)[169283]
    # TODO: Make assertions about result
    # print("tiles:", tiles)
    # x = int(tiles[0]['xStart'])
    # fields = tiles[0]['fields']
    # TODO: Make assertions


def test_gene_annotations():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'exon_unions_mm10.bed')
    f = tempfile.NamedTemporaryFile(delete=False)

    result = runner.invoke(
        cca.bedfile,
        [input_file,
         '--max-per-tile', '20', '--importance-column', '5',
         '--delimiter', '\t',
         '--assembly', 'mm10', '--output-file', f.name])

    # import traceback
    a, b, tb = result.exc_info
    '''
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    '''

    rows = cdt.get_tiles(f.name, 0, 0)
    assert(len(rows[0]) == 2)

    rows = cdt.get_tiles(f.name, 11, 113)
    assert(rows[113][0]['fields'][3] == 'Lrp1b')

    rows = cdt.get_tiles(f.name, 11, 112)
    assert(rows[112][0]['fields'][3] == 'Lrp1b')


def test_random_importance():
    # check that when aggregating using random importance, all values that
    # are in a higher resolution tile are also in the lower resolution
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data',
                         '25435_PM15-000877_SM-7QK6O.seg')

    result = runner.invoke(
        cca.bedfile,
        [input_file,
         '--max-per-tile', '2', '--importance-column', 'random',
         '--assembly', 'b37', '--has-header', '--output-file', f.name])

    # import traceback
    a, b, tb = result.exc_info
    '''
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    '''

    cdt.get_tileset_info(f.name)
    # print("tileset_info:", tileset_info)
    # TODO: Make assertions about result

    cdt.get_tiles(f.name, 0, 0)
    # print("rows:", rows)
    # TODO: Make assertions about result

    list(cdt.get_tiles(f.name, 1, 0).values()) + \
        list(cdt.get_tiles(f.name, 1, 1).values())
    # print('rows:', rows)
    # TODO: Make assertions about result

    # check to make sure that tiles in the higher zoom levels
    # are all present in lower zoom levels
    found = {}
    for row in cdt.get_tiles(f.name, 5, 15).values():
        for rect in row:
            found[rect['xStart']] = False

    for row in cdt.get_tiles(f.name, 6, 30).values():
        for rect in row:
            if rect['xStart'] in found:
                found[rect['xStart']] = True

    for row in cdt.get_tiles(f.name, 6, 31).values():
        for rect in row:
            if rect['xStart'] in found:
                found[rect['xStart']] = True

    for key, value in found.items():
        assert(value)

    pass


def test_no_chromosome_limit():
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data',
                         'geneAnnotationsExonsUnions.short.bed')

    result = runner.invoke(
        cca.bedfile,
        [input_file,
         '--max-per-tile', '60', '--importance-column', '5',
         '--assembly', 'hg19',
         '--output-file', f.name])

    # import traceback
    '''
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    '''
    a, b, tb = result.exc_info

    rows = cdt.get_tiles(f.name, 0, 0)[0]
    foundOther = False

    for row in rows:
        if row['fields'][0] != 'chr1':
            # print("row", row)
            assert(row['xStart'] > 200000000)
        if row['fields'][0] != 'chr14':
            foundOther = True
        break
    # make sure there's chromosome other than 14 in the output
    assert(foundOther)

    os.remove(f.name)
    pass


def test_chromosome_limit():
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data',
                         'geneAnnotationsExonsUnions.short.bed')

    runner.invoke(
        cca.bedfile,
        [input_file,
         '--max-per-tile', '60', '--importance-column', '5',
         '--assembly', 'hg19', '--chromosome', 'chr14',
         '--output-file', f.name])
    # TODO: Make assertions about result

    # print('output:', result.output, result)
    rows = cdt.get_tiles(f.name, 0, 0)[0]

    for row in rows:
        assert(row['fields'][0] == 'chr14')

    os.remove(f.name)
    pass


def test_float_importance():
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'test_float_importance.bed')

    runner.invoke(
        cca.bedfile,
        [input_file,
         '--max-per-tile', '2', '--importance-column', '4',
         '--assembly', 'hg38', '--no-header', '--output-file', f.name])
    # TODO: Make assertions about result


"""
def test_get_tiles():
    f = h5py.File('test/sample_data/cnv.hibed')
    data = cht.get_discrete_data(f, 22, 48)

    assert(len(data) > 0)

    data = cht.get_discrete_data(f, 22, 50)
    assert(len(data) > 0)

    data = cht.get_discrete_data(f, 0, 0)
    assert(len(data) == 100)

def check_tile_for_duplicate_entries(discrete_data):
    '''
    Make sure that there are no entries with the same UID in any tile.
    '''
    seen = set()

    for i,d in enumerate(discrete_data):
        uid = d[-2]

        if uid in seen:
            # print("seen uid:", uid)
            # print("d:", d)
            return False

        # print("adding uid:", uid, d[:3])
        seen.add(uid)

    return True


def test_tile_ranges():
    f = h5py.File('test/sample_data/cnv.hibed')

    data11 = cht.get_discrete_data(f, 11, 6)
    assert(check_tile_for_duplicate_entries(data11) == True)

    max_length_11 = max([int(d[2]) - int(d[1]) for d in data11])
    # print("data11:", max_length_11)

    data10 = cht.get_discrete_data(f, 10, 3)
    max_length_10 = max([int(d[2]) - int(d[1]) for d in data10])
    # print("data10:", max_length_10)

    # more zoomed out tiles should have longer tiles than more
    # zoomed in tiles
    assert(max_length_10 >= max_length_11)

    d1 = cht.get_discrete_data(f, 11, 5)
    # print("d1:", len(d1))
    # print("dv:", [x for x in d1 if (int(x[1]) < 12000000
    # and int(x[2]) > 12000000)])

    d3 = cht.get_discrete_data(f, 12, 10)
    # print("d2:", len(d3))

    d4 = cht.get_discrete_data(f, 12, 11)
    # print("d3:", len(d4))

def test_limit_by_chromosome():

"""
