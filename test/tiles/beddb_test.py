import clodius.tiles.beddb as hgbe
import json
import os.path as op


def test_get_tiles():
    filename = op.join('data', 'corrected.geneListwithStrand.bed.multires')

    ret = hgbe.tiles(filename, ['x.1.0', 'x.1.1'])


def test_list_items():
    filename = op.join('data', 'gene_annotations.short.db')

    ret = hgbe.list_items(filename, 0, 100000000, max_entries=100)
    # print('ret:', ret)
