import clodius.tiles.bigwig as hgbi
import os.path as op

def test_bigwig_tiles():
    filename = op.join('data', 'wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig')

    meanval = hgbi.tiles(filename, ['x.0.0'])
    minval = hgbi.tiles(filename, ['x.0.0.min'])
    maxval = hgbi.tiles(filename, ['x.0.0.max'])
    assert meanval[0][1]['max_value'] > minval[0][1]['max_value']
    assert maxval[0][1]['max_value'] > meanval[0][1]['max_value']


def test_tileset_info():
    filename = op.join('data', 'wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig')

    tileset_info = hgbi.tileset_info(filename)
    # print('tileset_info', tileset_info)
