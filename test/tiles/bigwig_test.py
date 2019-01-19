import clodius.tiles.bigwig as hgbi
import os.path as op


def test_bigwig_tiles():
    filename = op.join(
      'data',
      'wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig'
    )

    meanval = hgbi.tiles(filename, ['x.0.0'])
    minval = hgbi.tiles(filename, ['x.0.0.min'])
    maxval = hgbi.tiles(filename, ['x.0.0.max'])
    assert meanval[0][1]['max_value'] > minval[0][1]['max_value']
    assert maxval[0][1]['max_value'] > meanval[0][1]['max_value']


def test_tileset_info():
    filename = op.join(
      'data',
      'wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig'
    )

    tileset_info = hgbi.tileset_info(filename)

    assert len(tileset_info['aggregation_modes']) == 4
    assert tileset_info['aggregation_modes']['mean']
    assert tileset_info['aggregation_modes']['min']
    assert tileset_info['aggregation_modes']['max']
    assert tileset_info['aggregation_modes']['std']
    # print('tileset_info', tileset_info)

def test_natsorted():
    chromname_tests = [
        {
            'input': ['2', '3', '4', 'm', 'x', '1', 'y'],
            'expected': ['1', '2', '3', '4', 'x', 'y', 'm']
        },
        {
            'input': ['chr1', 'chr4', 'chr5', 'chr2', 'chr3', 'chrMT', 'chrY', 'chrX'],
            'expected': ['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chrX', 'chrY', 'chrMT']
        }
    ]

    for test in chromname_tests:
        sorted_output = hgbi.natsorted(test['input'])
        assert sorted_output == test['expected'], 'Sorted output was %s\nExpected: %s' % (sorted_output, test['expected'])
