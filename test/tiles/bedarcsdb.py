import collections as col
import clodius.tiles.bedarcsdb as hgbad
import os.path as op


def get_counts(filename, zoom, pos):
    data = hgbad.tiles(filename, ["b.{}.{}".format(zoom, pos)])
    # print('data:', len(data[0][1][pos]))
    counts = col.defaultdict(int)

    for i, d in enumerate(data[0][1][pos]):
        chr1, chr2 = sorted([d['fields'][0], d['fields'][3]])
        # print('d:', d)

        counts[(chr1, chr2)] += 1
    return counts


def test_bedarcsdb_tiles1():
    filename = op.join(
        'data',
        '9ae0744a-9bc1-4cd7-b7cf-c6569ed9e4aa'
        '.pcawg_consensus_1.6.161022.somatic.sv.bedpe.multires.db')

    tiles_to_get = [(2, 2)]

    for ttg in tiles_to_get:
        get_counts(filename, *ttg)
        # print(*ttg, counts)
    # counts1 = get_counts(filename, 0,0)
    # counts1 = get_counts(filename, 1,1)
    # counts2 = get_counts(filename, 2,2)
    # counts3 = get_counts(filename, 2,3)

    # print('1,1', counts1)
    # print('2,2', counts2)
    # print('2,3', counts3)

    # print('1', counts1[('21', 'X')])
    # print('2', counts1[('21', 'X')])

    # print("1,1", get_counts(filename, 1,1))
    # print('2,2', get_counts(filename, 2,2))

    '''
    data = hgbad.tiles(filename, ["b.0.0"])
    # print('data:', len(data[0][1][0]))
    counts = col.defaultdict(int)

    for i,d in enumerate(data[0][1][0]):
        chr1,chr2 = sorted([d['fields'][0], d['fields'][3]])
        # print('d:', d)

        counts[(chr1,chr2)] += 1
    # print('counts:', counts)

    # print('counts:', counts)
    return

    data = hgbad.tiles(filename, ["b.4.11".format(pos)])
    # print('data:', len(data[0][1][11]))
    counts = col.defaultdict(int)

    for d in data[0][1][11]:
        chr1,chr2 = sorted([d['fields'][0], d['fields'][3]])

        counts[(chr1,chr2)] += 1
    # print('counts:', counts)

    data = hgbad.tiles(filename, ["b.4.10".format(pos)])
    # print('data:', len(data[0][1][10]))
    counts = col.defaultdict(int)

    for i,d in enumerate(data[0][1][10]):
        chr1,chr2 = sorted([d['fields'][0], d['fields'][3]])
        # print('d:', d)

        counts[(chr1,chr2)] += 1
    # print('counts:', counts)
    '''


def test_bedarcsdb_tiles():
    '''
    Retrieve a 1D tile from a 2d file
    '''
    filename = op.join('data', 'arrowhead_domains_short.txt.multires.db')

    pos = 2
    hgbad.tiles(filename, ["b.2.{}".format(pos)])

    # TODO: Do something with the data we get back?
