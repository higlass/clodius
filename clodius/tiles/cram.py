# import pysam

from clodius.tiles.bam import alignment_tileset_info
from clodius.tiles.bam import alignment_tiles


def tileset_info(filename, chromsizes):
    samfile = pysam.AlignmentFile(filename, "rc")

    return alignment_tileset_info(samfile, chromsizes)


def tiles(
    filename, tile_ids, index_filename=None, chromsizes=None, max_tile_width=None
):
    samfile = pysam.AlignmentFile(filename, "rc", index_filename=index_filename)

    return alignment_tiles(
        samfile, tile_ids, index_filename=None, chromsizes=None, max_tile_width=None
    )
