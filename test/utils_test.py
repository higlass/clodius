from __future__ import print_function

import os.path as op

import clodius.utils as cu
from clodius.tiles.utils import (
    abs2genome_fn,
    parse_tile_id,
    parse_tile_position,
    TilesetInfo,
)


def test_infer_filetype():
    assert cu.infer_filetype("blah.gff") == "gff"
    assert cu.infer_filetype("blah.gff.gz") == "gff"
    assert cu.infer_filetype("blah.xyz") == None
    assert cu.infer_filetype("blah.bam") == "bam"
    assert cu.infer_filetype("blah.bed.bgz") == "bedfile"
    assert cu.infer_filetype("blah.bed") == "bedfile"


def test_infer_datatype():
    assert cu.infer_datatype("gff") == "bedlike"
    assert cu.infer_datatype("cooler") == "matrix"
    assert cu.infer_datatype("bedfile") == "bedlike"
    assert cu.infer_datatype("bam") == "reads"


def test_abs2genome_fn():
    fai_filename = op.join(
        "data", "GCA_000350705.1_Esch_coli_KTE11_V1_genomic.short.fna.fai"
    )
    sections = list(abs2genome_fn(fai_filename, 0, 1000))

    assert len(sections) == 3
    assert sections[0].end == 640


def test_parse_tile_position():
    tsinfo = TilesetInfo(
        max_width=2**16,
        max_zoom=4,
        min_pos=[0, 0],
        max_pos=[2**15 + 10, 2**15 + 10],
    )

    x = parse_tile_position([1, 2], tsinfo)

    assert x.zoom == 1
    assert x.position[0] == 2
    assert x.start[0] == 65536
    assert x.end[0] == 98304


def test_parse_tile_id():
    tsinfo = TilesetInfo(
        max_width=2**16,
        max_zoom=4,
        min_pos=[0, 0],
        max_pos=[2**15 + 10, 2**15 + 10],
    )
    x = parse_tile_id("uid.1.2", tsinfo)

    assert x.zoom == 1
    assert x.position[0] == 2
    assert x.start[0] == 65536
