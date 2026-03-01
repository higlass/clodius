import os.path as op

import clodius.chromosomes as cc
import clodius.tiles.bedfile as ctb

import pytest


@pytest.mark.parametrize(
    "file",
    [
        # "test.vcf",
        "test.1.vcf.gz"
    ],
)
def test_vcf_tiles(file):
    valid_filename = op.join("data", file)
    chromsizes_fn = op.join("data", "chm13v1.chrom.sizes")

    chromsizes = cc.chromsizes_as_series(chromsizes_fn)

    tiles = ctb.tiles(
        valid_filename,
        ["x.0.0"],
        chromsizes,
        index_filename=None,
        settings={"filetype": "vcf"},
    )

    ends = set()
    starts = set()

    # Make sure the tile starts are after the tile ends
    # and keep track of how many different starts and ends
    # there are
    for t in tiles[0][1]:
        starts.add(t["xStart"])
        ends.add(t["xEnd"])

        assert t["xStart"] < t["xEnd"]

    assert len(ends) > 1
    assert len(tiles) > 0

    # try as file pointer
    with open(valid_filename, "rb") as f:
        tiles = ctb.tiles(
            f,
            ["x.0.0"],
            chromsizes,
            index_filename=None,
            settings={"filetype": "vcf"},
        )

    assert len(tiles) > 0
