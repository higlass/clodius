import clodius.tiles.beddb as hgbe
import os.path as op


def test_get_tiles():
    filename = op.join("data", "corrected.geneListwithStrand.bed.multires")

    hgbe.tiles(filename, ["x.1.0", "x.1.1"])
    # TODO: Do something with the return value


def test_list_items():
    filename = op.join("data", "gene_annotations.short.db")

    items = hgbe.list_items(filename, 0, 100000000, max_entries=100)
    assert "name" not in items[0]
    # TODO: Do something with the return value


def test_name_in_tile():
    filename = op.join("data", "geneAnnotationsExonUnions.1000.bed.v3.beddb")

    tiles = hgbe.tiles(filename, ["x.1.0", "x.1.1"])

    assert "name" in tiles[0][1][0]
