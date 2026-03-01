from __future__ import print_function

import os.path as op

import clodius.tiles.gff as ctg

testdir = op.realpath(op.dirname(__file__))


def test_tileset_info():
    filename = op.join("data", "GCA_002918705.1_ASM291870v1_genomic.gff.gz")

    tsinfo = ctg.tileset_info(filename)

    assert "max_zoom" in tsinfo


def test_tiles():
    filename = op.join("data", "GCA_002918705.1_ASM291870v1_genomic.gff.gz")

    tiles = ctg.tiles(filename, ["x.0.0"])

    assert len(tiles) == 1
    assert tiles[0][0] == "x.0.0"

    assert len(tiles[0][1]["genes"].keys()) > 20

    tiles1 = ctg.tiles(filename, ["x.1.0"])
    assert len(tiles1[0][1]["genes"].keys()) < len(tiles[0][1]["genes"].keys())


def test_indexed_tiles():
    filename = op.join("data", "genomic.10k.gff.gz")
    index = op.join("data", "genomic.10k.gff.gz.tbi")

    tiles = ctg.tiles(filename, ["x.0.0"], index_filename=index)
    assert len(tiles) == 1

    # genes
    assert len(tiles[0][1]["genes"].keys()) > 10
    # transcripts
    assert len(tiles[0][1]["transcripts"].keys()) > 10
