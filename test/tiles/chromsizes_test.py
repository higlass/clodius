import os.path as op

import clodius.tiles.chromsizes as ctcs
from clodius.models.tileset_info import TilesetInfo


def test_get_tileset_info():
    filename = op.join("data", "hg38.chrom.sizes")

    tsinfo = TilesetInfo(**ctcs.tileset_info(filename))

    assert tsinfo.max_width > 100
    assert len(tsinfo.chromsizes) > 2
    # TODO: Do something with the return value
