import hashlib
import random
import h5py
import json

from clodius.tiles.utils import tiles_wrapper_1d

from clodius.tiles.utils import (
    calc_max_width,
    interval_to_chrom_tiles,
    genome_tile_to_intervals,
)

import math
from clodius.tiles.utils import TilesetInfo


def tile_entries_sorter(x):
    # return x["zoom"], x["xEnd"] - x["xStart"]
    return x["zoom"], x["uid"]


MAX_PER_TILE = 4096


def tileset_info(filename, chromsizes):
    max_zoom = math.ceil(math.log(sum(chromsizes)) / math.log(2))
    max_width = 2**max_zoom

    chromsizes_list = [[chrom, int(size)] for chrom, size in chromsizes.items()]

    with h5py.File(filename, "r") as f:
        max_per_tile = f["info"].attrs["max_per_tile"]

    return {
        "max_width": max_width,
        "max_zoom": int(max_zoom),
        "chromsizes": chromsizes_list,
        "min_pos": [0],
        "max_pos": [max_width],
        "max_per_tile": int(max_per_tile),
    }


def single_chromosome_tile(
    filename, chromsizes, tsinfo: dict, chrom: str, z: int, x: int
):
    f = h5py.File(filename, "r")
    max_per_tile = tsinfo["max_per_tile"]
    css = chromsizes.cumsum().shift().fillna(0).to_dict()
    chrom_len = chromsizes.to_dict()[chrom]

    # print("max_per_tile", max_per_tile)
    # print("chrom", chrom, "z", z, "x", x)
    max_width = calc_max_width(chrom_len)
    # print("max_width:", max_width)
    tile_width = max_width / 2**z
    # print("tile_width", tile_width)
    tile_start = tile_width * x
    tile_end = tile_width * (x + 1)
    #     print('chromsizes:', chromsizes)
    #     print("max_per_tile:", max_per_tile)
    #     print("sct", z, x)
    # print("tile_start", tile_start / 1e6, tile_end / 1e6)
    items = []
    tile_pos = x

    if not chrom in f["values"]:
        # No entries for this chromosome
        return []

    if str(z) in f["values"][chrom]:
        # If the requested zoom is higher than the max then we just
        # return the next lowest zoom
        items += [
            (z, x)
            for x in list(
                f["values"][chrom][str(z)][
                    tile_pos * max_per_tile : (tile_pos + 1) * max_per_tile
                ]
            )
            if len(x)
        ]

    while z > 0:
        z -= 1
        tile_pos //= 2

        if str(z) in f["values"][chrom]:
            # If the requested zoom is higher than the max then we just
            # return the next lowest zoom
            items += [
                (z, x)
                for x in list(
                    f["values"][chrom][str(z)][
                        tile_pos * max_per_tile : (tile_pos + 1) * max_per_tile
                    ]
                )
                if len(x)
            ]
    formatted = []

    for z, row in items:
        row = json.loads(row.decode("utf8"))
        # print("row", row["line"])
        parts = row["line"].split("\t")
        importance = row["importance"]

        start = int(parts[1])
        end = int(parts[2])

        if not (end > tile_start and start < tile_end):
            #             print("ts", tile_start, tile_end)
            #             print("se", start, end)
            #             print("no intersection")
            # doesn't intersect tile
            continue

        ret = {
            "uid": hashlib.md5(row["line"].encode("utf-8")).hexdigest(),
            "zoom": z,
            "xStart": css[parts[0]] + int(parts[1]),
            "xEnd": css[parts[0]] + int(parts[2]),
            "chrOffset": css[parts[0]],
            "importance": importance,
            "fields": parts,
        }
        formatted += [ret]

    # sorted_formatted = sorted(formatted, key=lambda x: (x["zoom"], x["uid"]))
    sorted_formatted = sorted(formatted, key=tile_entries_sorter)
    return sorted_formatted[:max_per_tile]


def single_genome_tile(filename, chromsizes, tsinfo, z, x):
    chrom_tile_poss = []
    # print("chromsizes", chromsizes.index)
    intervals = genome_tile_to_intervals(
        filename, chromsizes, TilesetInfo.parse_obj(tsinfo), z, x
    )
    for interval in intervals:
        if interval[0] >= len(tsinfo["chromsizes"]):
            break
        chrom_name = tsinfo["chromsizes"][interval[0]][0]
        chrom_size = tsinfo["chromsizes"][interval[0]][1]

        chrom_tile_poss += [
            (chrom_name, cz, cx)
            for (cz, cx) in interval_to_chrom_tiles(
                interval[1], interval[2], chrom_size
            )
        ]

    # print("chrom_tile_poss", chrom_tile_poss)
    chrom_tiles = []
    for chrom, cz, cx in chrom_tile_poss:
        chrom_tile = single_chromosome_tile(filename, chromsizes, tsinfo, chrom, cz, cx)
        chrom_tiles += chrom_tile

    chrom_tiles = sorted(chrom_tiles, key=tile_entries_sorter)[:MAX_PER_TILE]
    # print("len(chrom_tiles)", len(chrom_tiles))
    return chrom_tiles


def tiles(filename, tile_ids, chromsizes):
    tsinfo = tileset_info(filename, chromsizes)

    return tiles_wrapper_1d(
        tile_ids, lambda z, x: single_genome_tile(filename, chromsizes, tsinfo, z, x)
    )


# tileset_info(filename, chromsizes)
# tile0 = single_genome_tile(filename, chromsizes, tsinfo, 1, 0)
