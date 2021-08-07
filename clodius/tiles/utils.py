import functools as ft
import os.path as op
import re
from typing import List, Optional

import numpy as np
from pydantic import BaseModel, validator

from clodius.chromosomes import load_chromsizes


def partition_by_adjacent_tiles(tile_ids, dimension=2):
    """
    Partition a set of tile ids into sets of adjacent tiles. For example,
    if we're requesting a set of four tiles that form a rectangle, then
    those four tiles will become one set of adjacent tiles. Non-contiguous
    tiles are not grouped together.

    Parameters
    ----------
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0.1) identifying the tiles
        to be retrieved
    dimension: int
        The dimensionality of the tiles

    Returns
    -------
    tile_lists: [tile_ids, tile_ids]
        A list of tile lists, all of which have tiles that
        are within 1 position of another tile in the list
    """
    tile_id_lists = []

    for tile_id in sorted(
        tile_ids, key=lambda x: [int(p) for p in x.split(".")[2 : 2 + dimension]]
    ):
        tile_id_parts = tile_id.split(".")

        # exclude the zoom level in the position
        # because the tiles should already have been partitioned
        # by zoom level
        tile_position = list(map(int, tile_id_parts[2:4]))

        added = False

        for tile_id_list in tile_id_lists:
            # iterate over each group of adjacent tiles
            for ct_tile_id in tile_id_list:
                ct_tile_id_parts = ct_tile_id.split(".")
                ct_tile_position = list(map(int, ct_tile_id_parts[2 : 2 + dimension]))
                far_apart = False

                # iterate over each dimension and see if this tile is close
                for p1, p2 in zip(tile_position, ct_tile_position):
                    if abs(int(p1) - int(p2)) > 1:
                        # too far apart can't be part of the same group
                        far_apart = True

                if not far_apart:
                    # no position was too far
                    tile_id_list += [tile_id]
                    added = True
                    break

            if added:
                break
        if not added:
            tile_id_lists += [[tile_id]]

    return tile_id_lists


def infer_filetype(filename):
    _, ext = op.splitext(filename)

    if ext.lower() == ".bw" or ext.lower() == ".bigwig":
        return "bigwig"
    elif ext.lower() == ".mcool" or ext.lower() == ".cool":
        return "cooler"
    elif ext.lower() == ".htime":
        return "time-interval-json"
    elif ext.lower() == ".hitile":
        return "hitile"
    elif ext.lower() == ".beddb":
        return "beddb"
    elif ext.lower() == ".mv5":
        return "multivec"

    return None


def infer_datatype(filetype):
    if filetype == "cooler":
        return "matrix"
    if filetype == "bigwig":
        return "vector"
    if filetype == "time-interval-json":
        return "time-interval"
    if filetype == "hitile":
        return "vector"
    if filetype == "beddb":
        return "bedlike"


def tiles_wrapper_2d(tile_ids, tiles_function):
    tile_values = []

    for tile_id in tile_ids:
        parts = tile_id.split(".")

        if len(parts) < 4:
            raise IndexError("Not enough tile info present")

        z, x, y = map(int, [parts[1], parts[2], parts[3]])

        tile_values += [(tile_id, tiles_function(z, x, y))]

    return tile_values


def bundled_tiles_wrapper_2d(tile_ids, tiles_function):
    """
    Bundle adjacent tile requests so that they can be
    processed concurrently. This is helpful for function
    that require scanning a dataset. It's faster to filter
    a large region and then break it down into individual
    tiles than to go over the entire dataset and filter
    individual tiles multiple times.
    """
    tile_values = []

    partitioned_tile_lists = partition_by_adjacent_tiles(tile_ids)

    for tile_group in partitioned_tile_lists:
        zoom_level = int(tile_group[0].split(".")[1])
        tileset_id = tile_group[0].split(".")[0]

        tile_positions = [[int(x) for x in t.split(".")[2:4]] for t in tile_group]

        minx = min([t[0] for t in tile_positions])
        maxx = max([t[0] for t in tile_positions])

        miny = min([t[1] for t in tile_positions])
        maxy = max([t[1] for t in tile_positions])

        tf = tiles_function(
            zoom_level, minx, miny, width=maxx - minx + 1, height=maxy - miny + 1
        )

        tile_values += [
            ("{}.{}".format(tileset_id, ".".join(map(str, tile_position))), data)
            for (tile_position, data) in tf
        ]

    return tile_values


def tile_bounds(tsinfo, z, x, y, width=1, height=1):
    """
    Get the coordinate boundaries for the given tile.

    Parameters:
    -----------
    tsinfo: { min_pos: [], max_pos [] }
        Tileset info containing the bounds of the dataset
    z: int
        The zoom level
    x: int
        The x position
    y: int
        The y position
    width: int
        Return bounds for a region encompassing multiple tiles
    height: int
        Return bounds for a region encompassing multiple tiles
    """
    min_pos = tsinfo["min_pos"]
    max_pos = tsinfo["max_pos"]

    max_width = max(max_pos[0] - min_pos[0], max_pos[1] - min_pos[1])

    tile_width = max_width / 2 ** z
    from_x = min_pos[0] + x * tile_width
    to_x = min_pos[0] + (x + width) * tile_width

    from_y = min_pos[1] + y * tile_width
    to_y = min_pos[1] + (y + height) * tile_width

    return [from_x, from_y, to_x, to_y]


class TilesetInfo(BaseModel):
    max_zoom: int
    max_width: int
    max_pos: List[int]
    min_pos: List[int]

    @validator("max_zoom")
    def max_zoom_zero_or_greater(cls, v):
        """Check to make sure the zoom level is 0 or greater."""
        if v < 0:
            raise ValueError("The zoom level must be greater than or equal to 0")
        return int(v)

    @validator("max_width")
    def max_width_greater_than_zero(cls, v):
        """Check to make sure the max_width is greater than 0"""
        if v <= 0:
            raise ValueError("The max_width must be greater than 0")
        return int(v)


class TileInfo(BaseModel):
    zoom: int
    position: List[int]
    width: Optional[int]
    start: List[int]
    end: List[int]

    @validator("zoom")
    def zoom_zero_or_greater(cls, v):
        """Check to make sure the zoom level is 0 or greater."""
        if v < 0:
            raise ValueError("The zoom level must be greater than 0")
        return int(v)


def parse_tile_id(tile_id, tsinfo):
    tile_id_parts = tile_id.split("|")[0].split(".")
    tile_position = list(map(int, tile_id_parts[1:3]))
    zoom_level = int(tile_id_parts[1])

    tile_width = tsinfo.max_width / 2 ** int(tile_position[0])

    starts = [
        pos * (tsinfo.max_width / 2 ** zoom_level) + tsinfo.min_pos[i]
        for (i, pos) in enumerate(tile_position[1:])
    ]
    ends = [
        (pos * (tsinfo.max_width / 2 ** zoom_level) + tsinfo.min_pos[i] + tile_width)
        for (i, pos) in enumerate(tile_position[1:])
    ]

    return TileInfo(
        zoom=zoom_level,
        position=tile_position[1:],
        width=tile_width,
        start=starts,
        end=ends,
    )


def abs2genomic(chromsizes, start_pos, end_pos):
    """
    Convert absolute coordinates to genomic coordinates

    Parameters:
    -----------
    chromsizes: [[chrom, size],...]
        A list of chromosome sizes associated with this tileset
    start_pos: int
        The absolute start coordinate
    end_pos: int
        The absolute end coordinate
    """
    abs_chrom_offsets = np.r_[0, np.cumsum(chromsizes)]
    cid_lo, cid_hi = (
        np.searchsorted(abs_chrom_offsets, [start_pos, end_pos], side="right") - 1
    )
    rel_pos_lo = start_pos - abs_chrom_offsets[cid_lo]
    rel_pos_hi = end_pos - abs_chrom_offsets[cid_hi]
    start = rel_pos_lo
    for cid in range(cid_lo, cid_hi):
        yield cid, start, chromsizes[cid]
        start = 0
    yield cid_hi, int(start), int(rel_pos_hi)


class ChromosomeInterval(BaseModel):
    cid: int
    name: str
    start: int
    end: int


def abs2genome_fn(chromsizes_filename, start, end):
    """Convert an absolute genomic range to sections of genomic ranges.

    E.g. (1000,2000) => [('chr1', 1000, 1500), ('chr2', 1500, 2000)]
    """
    (chrom_info, chrom_names, chrom_sizes) = load_chromsizes(chromsizes_filename)

    for cid, start, end in abs2genomic(chrom_sizes, start, end):
        try:
            yield ChromosomeInterval(
                cid=cid, name=chrom_names[cid], start=start, end=end
            )
        except IndexError:
            # we've gone beyond the last chromosome so stop iterating
            return
    yield cid_hi, start, rel_pos_hi


def get_quadtree_depth(chromsizes, tile_size_bp):
    """
    Depth of quad tree necessary to tesselate the concatenated genome with quad
    tiles such that linear dimension of the tiles is a preset multiple of the
    genomic resolution.

    Parameters:
    -----------
    chromsizes: pandas.Series
        A series representation of the chromosome sizes
    tile_size_bp: int
        The size of each tile in the tileset
    """
    min_tile_cover = np.ceil(sum(chromsizes) / tile_size_bp)
    return int(np.ceil(np.log2(min_tile_cover)))


def natcmp(x, y):
    if x.find("_") >= 0:
        x_parts = x.split("_")
        if y.find("_") >= 0:
            # chr_1 vs chr_2
            y_parts = y.split("_")

            return natcmp(x_parts[1], y_parts[1])
        else:
            # chr_1 vs chr1
            # chr1 comes first
            return 1
    if y.find("_") >= 0:
        # chr1 vs chr_1
        # y comes second
        return -1

    _NS_REGEX = re.compile(r"(\d+)", re.U)
    x_parts = tuple([int(a) if a.isdigit() else a for a in _NS_REGEX.split(x) if a])
    y_parts = tuple([int(a) if a.isdigit() else a for a in _NS_REGEX.split(y) if a])

    # order of these parameters is purposefully reverse how they should be
    # ordered
    for key in ["m", "y", "x"]:
        if key in y.lower():
            return -1
        if key in x.lower():
            return 1

    try:
        if x_parts < y_parts:
            return -1
        elif y_parts > x_parts:
            return 1
        else:
            return 0
    except TypeError:
        return 1


def natsorted(iterable):
    """
    Sort an iterable by natural genomic order
    """
    return sorted(iterable, key=ft.cmp_to_key(natcmp))
