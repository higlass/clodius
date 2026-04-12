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


def test_single_tile_default_feature_type():
    """single_tile defaults to filtering on 'gene' features."""
    filename = op.join("data", "GCA_002918705.1_ASM291870v1_genomic.gff.gz")
    chromsizes = ctg.gff_chromsizes(filename)
    tsinfo = ctg.tileset_info(filename, chromsizes)

    result = ctg.single_tile(filename, chromsizes, tsinfo, z=0, x=0)

    assert len(result) > 0
    # Sanity-check: explicit feature_type="gene" produces the same count.
    result_explicit = ctg.single_tile(
        filename, chromsizes, tsinfo, z=0, x=0, settings={"feature_type": "gene"}
    )
    assert len(result) == len(result_explicit)


def test_single_tile_custom_feature_type():
    """single_tile respects a custom feature_type passed via settings."""
    filename = op.join("data", "GCA_002918705.1_ASM291870v1_genomic.gff.gz")
    chromsizes = ctg.gff_chromsizes(filename)
    tsinfo = ctg.tileset_info(filename, chromsizes)

    gene_result = ctg.single_tile(filename, chromsizes, tsinfo, z=0, x=0)
    cds_result = ctg.single_tile(
        filename, chromsizes, tsinfo, z=0, x=0, settings={"feature_type": "CDS"}
    )

    # CDS features should be present; their row UIDs must differ from gene UIDs
    # (both may hit the MAX_PER_TILE cap, so count comparison is not meaningful)
    assert len(cds_result) > 0
    gene_uids = {r["uid"] for r in gene_result}
    cds_uids = {r["uid"] for r in cds_result}
    assert gene_uids != cds_uids


def test_single_tile_unknown_feature_type():
    """single_tile returns an empty list when feature_type has no matches."""
    filename = op.join("data", "GCA_002918705.1_ASM291870v1_genomic.gff.gz")
    chromsizes = ctg.gff_chromsizes(filename)
    tsinfo = ctg.tileset_info(filename, chromsizes)

    result = ctg.single_tile(
        filename, chromsizes, tsinfo, z=0, x=0, settings={"feature_type": "nonexistent_feature"}
    )

    assert len(result) == 0


def test_indexed_tiles():
    filename = op.join("data", "genomic.10k.gff.gz")
    index = op.join("data", "genomic.10k.gff.gz.tbi")

    tiles = ctg.tiles(filename, ["x.0.0"], index_filename=index)
    assert len(tiles) == 1

    # genes
    assert len(tiles[0][1]["genes"].keys()) > 10
    # transcripts
    assert len(tiles[0][1]["transcripts"].keys()) > 10
