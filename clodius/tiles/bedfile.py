import functools as ft
import hashlib
import math
import os
import random

import pandas as pd
from pydantic import BaseModel
import io
import json

import clodius.tiles.tabix as ctt
import logging

from smart_open import open

# import pysam
from clodius.tiles.vcf import generic_regions
from clodius.utils import get_file_compression, TILE_OPTIONS_CHAR

logger = logging.getLogger(__name__)

cache = []


class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.tm = 0
        self.cache = {}
        self.lru = {}

    def get(self, key):
        if key in self.cache:
            self.lru[key] = self.tm
            self.tm += 1
            return self.cache[key]
        return None

    def set(self, key, value):
        if len(self.cache) >= self.capacity:
            # find the LRU entry
            old_key = min(self.lru.keys(), key=lambda k: self.lru[k])
            self.cache.pop(old_key)
            self.lru.pop(old_key)
        self.cache[key] = value
        self.lru[key] = self.tm
        self.tm += 1


cache = LRUCache(1)


def tileset_info(filename, chromsizes=None, index_filename=None):
    """

    Return the bounds of this tileset. The bounds should encompass the entire
    width of this dataset.

    So how do we know what those are if we don't know chromsizes? We can assume
    that the file is enormous (e.g. has a width of 4 trillion) and rely on the
    browser to pass in a set of chromsizes
    """

    # do this so that we can serialize the int64s in the numpy array
    chromsizes_list = []

    if chromsizes is None:
        return {
            "error": "No chromsizes found. Make sure the project has a chromsizes file or the assembly: tag is set"
        }
    for chrom, size in chromsizes.items():
        chromsizes_list += [[chrom, int(size)]]

    max_width = sum([c[1] for c in chromsizes_list])

    if not index_filename:
        if isinstance(filename, str):
            filesize = os.stat(filename).st_size
        else:
            # We're going to record the current position in the file
            # seek to the end to see how big it is and then seek back to
            # the original position
            orig_pos = filename.tell()
            filename.seek(0, io.SEEK_END)
            filesize = filename.tell()
            filename.seek(orig_pos)

        if filesize > 20e6:
            return {"error": "File too large (>20Mb), please index"}

    return {
        "max_width": max_width,
        "max_zoom": int(math.log(max_width) / math.log(2)),
        "chromsizes": chromsizes_list,
        "min_pos": [0],
        "max_pos": [max_width],
    }


def row_to_bedlike(row, css, orig_columns):
    ret = {
        "uid": row["ix"],
        "xStart": row["xStart"],
        "xEnd": row["xEnd"],
        "chrOffset": css[row[0]],
        "importance": random.random(),
        "fields": [r for r in row[orig_columns]],
    }

    return ret


def ts_hash(filename, chromsizes):
    cs_hash = hashlib.md5(str(chromsizes).encode("utf-8")).hexdigest()
    return f"{filename}.{cs_hash}"


def single_indexed_tile(file, index, chromsizes, tsinfo, z, x, tbx_index, settings):
    """Retrieve a single tile from an indexed bedfile."""
    from clodius.tiles.tabix import dataframe_tabix_fetcher

    css = chromsizes.cumsum().shift().fillna(0).to_dict()

    # try:
    df = ctt.single_indexed_tile(
        file,
        index,
        chromsizes,
        tsinfo,
        z,
        x,
        tbx_index=tbx_index,
        fetcher=dataframe_tabix_fetcher,
        max_results=settings.get("MAX_BEDFILE_ENTRIES"),
    )

    res = [x.split("\t") for x in df["raw"]]
    # except ValueError as err:
    #     return {"error": str(err)}

    formatted = []

    if "error" in res:
        # tile probably too large
        return res

    for row in res:
        parts = row
        if settings.get("filetype") == "vcf":
            xEnd = css[parts[0]] + int(parts[1]) + len(parts[3])
        else:
            xEnd = css[parts[0]] + int(parts[2])

        ret = {
            "uid": hashlib.md5("\t".join(row).encode("utf-8")).hexdigest(),
            "xStart": css[parts[0]] + int(parts[1]),
            "xEnd": xEnd,
            "chrOffset": css[parts[0]],
            "importance": random.random(),
            "fields": parts,
        }

        formatted += [ret]

    return formatted


def get_bedfile_values(filename, chromsizes, settings):
    """Return a processed bedfile containing a dataframe and
    and some other information."""
    cache = settings.get("cache")
    identifier = settings.get("filename")
    hash_ = None

    if not isinstance(filename, str):
        # we already have a file pointer
        filename = filename
    else:
        filename = open(filename, "rb", compression="disable")

    f = filename

    logger.info("bedfiles identifier: %s", identifier)

    val = None

    if identifier:
        hash_ = ts_hash(identifier, chromsizes)

        # hash the loaded data table so that we don't have to read the entire thing
        # and calculate cumulative start and end positions
        val = cache.get(hash_) if cache else None
        val = json.loads(val) if val else None

    if val is None:
        ## We have a file-like object, we need to rewing to the beginning
        f.seek(0)

        ## Then we have to figure out how it's compressed because we expect a
        ## file pointer with no compression enabled
        compression = get_file_compression(filename)

        t = pd.read_csv(
            filename,
            header=None,
            delimiter="\t",
            encoding="ISO-8859-1",
            comment="#",
            compression=compression,
        )

        orig_columns = list(t.columns)
        css = chromsizes.cumsum().shift().fillna(0).to_dict()

        # xStart and xEnd are cumulative start and end positions calculated
        # as if the chromosomes are concatenated from end to end
        t["chromStart"] = t[0].map(lambda x: css[x])

        t["xStart"] = t["chromStart"] + t[1]

        if settings.get("filetype") == "vcf":
            t["xEnd"] = t["chromStart"] + t[1] + len(t[3])
        else:
            t["xEnd"] = t["chromStart"] + t[2]
        t["ix"] = t.index

        val = {"rows": t.to_json(), "orig_columns": orig_columns, "css": css}
        if cache and hash_:
            cache.set(hash_, json.dumps(val))

    return val


def single_tile(filename, chromsizes, tsinfo, z, x, settings=None):
    """
    Available settings:

    {
        MAX_BEDFILE_ENTRIES: int
    }
    """
    if settings is None:
        settings = {}

    try:
        val = get_bedfile_values(filename, chromsizes, settings)
    except KeyError as ke:
        return {
            "error": f"Key error: (bedfile tab separated? correct chromsizes?) {str(ke)}"
        }

    t = pd.read_json(val["rows"])
    orig_columns = val["orig_columns"]
    css = val["css"]

    tileStart = x * tsinfo["max_width"] / 2**z
    tileEnd = (x + 1) * tsinfo["max_width"] / 2**z

    t = t.query(f"xEnd >= {tileStart} & xStart <= {tileEnd}")
    MAX_PER_TILE = settings.get("MAX_BEDFILE_ENTRIES") or 1024

    t = t.sample(MAX_PER_TILE) if len(t) > MAX_PER_TILE else t

    ret = t.apply(
        ft.partial(row_to_bedlike, css=css, orig_columns=orig_columns), axis=1
    )
    return list(ret.values)


def tiles(
    filename, tile_ids, chromsizes, index_filename, settings=None, single_tile_func=None
):
    if single_tile_func is None:
        single_tile_func = single_tile

    tsinfo = tileset_info(filename, chromsizes, index_filename)

    if settings is None:
        settings = {}

    tile_values = []

    if isinstance(filename, str):
        if index_filename:
            # If the file is indexed we need to disable compression so that
            # tabix indexing can retrieve the correct positions
            file = open(filename, "rb", compression="disable")
        else:
            # If the file isn't indexed, we're going to use a polars dataframe
            # to load it and that requires the compression to be resolved
            file = open(filename, "rb")
    else:
        file = filename

    if index_filename:
        tbx_index = ctt.load_tbi_idx(index_filename)

    for tile_id in tile_ids:
        tile_option_parts = tile_id.split(TILE_OPTIONS_CHAR)[1:]
        tile_no_options = tile_id.split(TILE_OPTIONS_CHAR)[0]
        tile_id_parts = tile_no_options.split(".")
        tile_position = list(map(int, tile_id_parts[1:3]))
        tile_options = dict([o.split(":") for o in tile_option_parts])

        if len(tile_position) < 2:
            raise IndexError("Not enough tile info present")

        z = tile_position[0]
        x = tile_position[1]

        if index_filename:
            values = single_indexed_tile(
                file,
                index_filename,
                chromsizes,
                tsinfo,
                z,
                x,
                tbx_index=tbx_index,
                settings=settings,
            )
        else:
            values = single_tile_func(file, chromsizes, tsinfo, z, x, settings=settings)

        tile_values += [(tile_id, values)]

    return tile_values


class BedfileEntry(BaseModel):
    chrom: str
    start: int
    end: int


def regions(filename, chromsizes, offset, limit, settings={}):
    """Return a list of regions in the range.

    Arguments:
        filename: The name of the file
        chromsizes: A dictionary containing the offsets of each chromosome
            from the start of the genome
        offset: The offset from the beginning of the file from which to start
            fetching entries
        limit: The total number of entries to fetch
    """
    vals = get_bedfile_values(filename, chromsizes, settings=settings)

    def row_iterator():
        for ix, row in pd.read_json(vals["rows"]).iterrows():
            yield {
                "uid": row["ix"],
                "chrOffset": row["chromStart"],
                "xStart": row["xStart"],
                "xEnd": row["xEnd"],
                "fields": list(row[vals["orig_columns"]].array),
            }

    return generic_regions(row_iterator(), offset, limit)
