import os.path as op

import clodius.tiles.fasta as ctf

fasta_filename = op.join("data", "GCA_000350705.1_Esch_coli_KTE11_V1_genomic.short.fna")
fai_filename = op.join(
    "data", "GCA_000350705.1_Esch_coli_KTE11_V1_genomic.short.fna.fai"
)


def test_tileset_info():
    tsinfo = ctf.tileset_info(fai_filename)

    assert "max_zoom" in tsinfo
    assert "max_width" in tsinfo


def test_multivec_tiles():
    tiles = ctf.multivec_tiles(
        fasta_filename, index_filename=fai_filename, tile_ids=["x.0.0"]
    )

    tsinfo = ctf.tileset_info(fai_filename)

    assert "shape" in tiles[0][1]


def test_sequence_tiles():

    tsinfo = ctf.tileset_info(fai_filename)

    tiles = ctf.sequence_tiles(
        fasta_filename, index_filename=fai_filename, tile_ids=["x.2.0"]
    )
    assert len(tiles[0][1]["sequence"]) == ctf.TILE_SIZE

    tiles = ctf.sequence_tiles(
        fasta_filename, index_filename=fai_filename, tile_ids=["x.0.0"]
    )
    assert len(tiles[0][1]["sequence"]) == tsinfo["max_pos"][0]
