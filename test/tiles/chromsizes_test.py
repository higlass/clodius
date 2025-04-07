import os.path as op

import clodius.tiles.chromsizes as ctcs
from clodius.models.tileset_info import TilesetInfo


def test_get_tileset_info():
    filename = op.join("data", "chromSizes.tsv")

    # Test loading tileset info using a filename
    tsinfo = TilesetInfo(**ctcs.tileset_info(filename))

    assert tsinfo.max_width > 100
    assert len(tsinfo.chromsizes) > 2

    with open(filename, "rb") as f:
        # Test loading using a file-like object
        tsinfo = TilesetInfo(**ctcs.tileset_info(f))

        assert tsinfo.max_width > 100
        assert len(tsinfo.chromsizes) > 2
