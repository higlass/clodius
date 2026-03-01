import itertools
import math
import random

import pandas as pd

import clodius.tiles.tabix as rtt
from clodius.tiles.bigwig import abs2genomic
from clodius.utils import TILE_OPTIONS_CHAR

# from pysam import VariantFile


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def generic_regions(fetcher, offset, limit):
    if offset:
        for i in range(offset):
            try:
                next(fetcher)
            except StopIteration:
                return {"offset": offset, "limit": limit, "results": [], "next": False}

    curr_page = next(grouper(limit, fetcher))

    try:
        # see if there's another page of results
        next_page = next(grouper(limit, fetcher))
        next_page = True
    except StopIteration:
        next_page = false

    ret = curr_page

    return (ret, next_page)


def regions(filename, chromsizes, offset, limit):
    """Return a list of regions in the range.

    Arguments:
        filename: The name of the file
        chromsizes: A dictionary containing the offsets of each chromosome
            from the start of the genome
        offset: The offset from the beginning of the file from which to start
            fetching entries
        limit: The total number of entries to fetch
    """
    vcf = VariantFile(filename)  # auto-detect input format

    fetcher = vcf.fetch()
    css = chromsizes.cumsum().shift().fillna(0).to_dict()

    def regions_iterator():
        for rec in fetcher:
            yield {
                "uid": rec.id,
                "chrOffset": css[rec.chrom],
                "xStart": css[rec.chrom] + rec.start,
                "xEnd": css[rec.chrom] + rec.stop,
                "fields": (rec.chrom, rec.start, rec.stop, str(rec)),
            }

    return generic_regions(regions_iterator(), offset, limit)


def tileset_info(filename, chromsizes):
    """

    Return the bounds of this tileset. The bounds should encompass the entire
    width of this dataset.

    So how do we know what those are if we don't know chromsizes? We can assume
    that the file is enormous (e.g. has a width of 4 trillion) and rely on the
    browser to pass in a set of chromsizes
    """

    # do this so that we can serialize the int64s in the numpy array
    chromsizes_list = []

    for chrom, size in chromsizes.items():
        chromsizes_list += [[chrom, int(size)]]

    max_width = sum([c[1] for c in chromsizes_list])
    MAX_TILE_WIDTH = 100000

    return {
        "max_width": max_width,
        "max_zoom": int(math.log(max_width) / math.log(2)),
        "chromsizes": chromsizes_list,
        "min_pos": [0],
        "max_pos": [max_width],
        "max_tile_width": MAX_TILE_WIDTH,
    }


# def tiles_wrapper(array, tile_ids, not_nan_array=None):
#     tile_values = []

#     for tile_id in tile_ids:
#         parts = tile_id.split(".")

#         if len(parts) < 3:
#             raise IndexError("Not enough tile info present")

#         z = int(parts[1])
#         x = int(parts[2])

#         ret_array = tiles(array, z, x, not_nan_array).reshape((-1))

#         tile_values += [(tile_id, ctf.format_dense_tile(ret_array))]

#     return tile_values


def single_tile(
    filename, index_filename, chromsizes, tsinfo, z, x, max_tile_width, tbx_index=None
):
    # TODO: replace this function with the one in clodius.tiles.tabix
    tile_width = tsinfo["max_width"] / 2**z

    if max_tile_width and tile_width > max_tile_width:
        return {"error": "Tile too wide"}

    query_size = 0

    start_pos = x * tsinfo["max_width"] / 2**z
    end_pos = (x + 1) * tsinfo["max_width"] / 2**z

    css = chromsizes.cumsum().shift().fillna(0).to_dict()

    vcf = VariantFile(
        filename, index_filename=index_filename
    )  # auto-detect input format

    cids_starts_ends = list(abs2genomic(chromsizes, start_pos, end_pos))
    ret_vals = []

    if tbx_index:
        for cid, start, end in cids_starts_ends:
            chrom = chromsizes.index[cid]

            query_size += rtt.est_query_size(tbx_index, chrom, int(start), int(end))

    MAX_QUERY_SIZE = 450000

    if query_size > MAX_QUERY_SIZE:
        return {"error": f"Tile too large {query_size}"}

    for cid, start, end in cids_starts_ends:
        chrom = chromsizes.index[cid]
        ret_vals += [
            {
                "uid": r.id,
                "importance": random.random(),
                "xStart": css[chrom] + r.start,
                "xEnd": css[chrom] + r.stop,
                "chrOffset": css[chrom],
                "fields": [r.chrom, r.start, r.stop, str(r)],
            }
            for r in vcf.fetch(str(chrom), int(start), int(end))
        ]

    return ret_vals


def tiles(filename, tile_ids, index_filename, chromsizes, max_tile_width=None):
    tsinfo = tileset_info(filename, chromsizes)

    tile_values = []

    index = None
    if index_filename:
        index = rtt.load_tbi_idx(index_filename)

    for tile_id in tile_ids:
        tile_option_parts = tile_id.split(TILE_OPTIONS_CHAR)[1:]
        tile_no_options = tile_id.split(TILE_OPTIONS_CHAR)[0]
        tile_id_parts = tile_no_options.split(".")
        tile_position = list(map(int, tile_id_parts[1:3]))

        if len(tile_position) < 2:
            raise IndexError("Not enough tile info present")

        tile_width = tsinfo["max_width"] / 2 ** int(tile_position[0])

        if max_tile_width and tile_width >= max_tile_width:
            # this tile is larger than the max allowed
            return [
                (
                    tile_id,
                    {
                        "error": f"Tile too large, no data returned. Max tile size: {max_tile_width}"
                    },
                )
            ]

        z = tile_position[0]
        x = tile_position[1]

        values = single_tile(
            filename,
            index_filename,
            chromsizes,
            tsinfo,
            z,
            x,
            max_tile_width,
            tbx_index=index,
        )

        tile_values += [(tile_id, values)]

    return tile_values
