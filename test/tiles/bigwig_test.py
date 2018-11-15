import clodius.tiles.bigwig as hgbi
import os.path as op

def test_bigwig_tiles():
    filename = op.join('data', 'wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig')

    hgbi.tiles(filename, ['x.0.0'])


def test_tileset_info():
    filename = op.join('data', 'wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig')

    tileset_info = hgbi.tileset_info(filename)
    # print('tileset_info', tileset_info)

