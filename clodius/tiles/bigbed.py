import bbi
import functools as ft
import logging
import numpy as np
import numpy.random as nr
import pandas as pd
import random
import clodius.tiles.bigwig as hgbw
from clodius.utils import TILE_OPTIONS_CHAR
import pybigtools
import slugid
from hashlib import md5

from concurrent.futures import ThreadPoolExecutor

DEFAULT_RANGE_MODE = "significant"
MIN_ELEMENTS = 1
MAX_ELEMENTS = 200
DEFAULT_SCORE = 0

logger = logging.getLogger(__name__)

range_modes = {}
range_modes["significant"] = {"name": "Significant", "value": "significant"}


def tileset_info(bbpath, chromsizes=None):
    ti = hgbw.tileset_info(bbpath, chromsizes)
    ti["range_modes"] = range_modes
    return ti


# def fetch_data(a):
#     (
#         bbpath,
#         binsize,
#         chromsizes,
#         range_mode,
#         min_elements,
#         max_elements,
#         cid,
#         start,
#         end,
#     ) = a

#     """
#     Retrieve tile data from a bigbed file.

#     This approach currently returns a subset of intervals within the bounds of the specified
#     query range.

#     The subset is determined, at this time, by using the population of scores in the score
#     column of the BED data to generate a quantile value that would allow, at most, a maximum
#     number of elements (either a default or specified value). Because intervals are discrete
#     elements, it is possible for a quantile to allow a few more elements than the desired
#     limit; in this case, a uniformly-random sample is drawn from the thresholded set without
#     replacement.

#     Parameters
#     ----------
#     bbpath: string
#         The path to the bigBed media file
#     binsize: integer
#         Resolution of a bin at a particular zoom level
#     chromsizes: [[chrom, size],...]
#         A 2d array containing chromosome names and sizes. Overrides the
#         chromsizes in chromsizes_map
#     range_mode: string or None
#         If specified, determines what rule is applied to intervals retrieved
#         over the specified chromosome, start, and end range
#     min_elements: integer
#         For fetched intervals, return no fewer than the specified number
#     max_elements: integer
#         For fetched intervals, return no more than the specified number
#     cid: integer
#         Index of chromosome associated with chromsizes
#     start: integer
#         Start position of interval query (relative to chromosome)
#     end: integer
#         End position of interval query (relative to chromosome)

#     Returns
#     -------
#     intervals: [{'chrOffset': integer, 'importance': integer, 'fields': [interval]}, ... ]
#         A list of beddb-like gene annotation objects
#     """

#     try:
#         chrom = chromsizes.index[cid]

#         fetch_factory = ft.partial(bbi.fetch_intervals, bbpath, chrom, start, end)

#         if range_mode == "significant":
#             intervals, intervals2 = fetch_factory(), fetch_factory()
#         else:
#             intervals, intervals2 = fetch_factory(), fetch_factory()

#     except IndexError:
#         # beyond the range of the available chromosomes
#         # probably means we've requested a range of absolute
#         # coordinates that stretch beyond the end of the genome
#         intervals, intervals2 = None, None

#     except KeyError:
#         # probably requested a chromosome that doesn't exist (e.g. chrM)
#         intervals, intervals2 = None, None

#     offset = 0
#     offsetIdx = 0
#     chrOffsets = {}
#     for chrSize in chromsizes:
#         chrOffsets[chromsizes.index[offsetIdx]] = offset
#         offset += chrSize
#         offsetIdx += 1

#     final_intervals = []
#     intervals_length = 0
#     scores = []

#     return [
#         {
#             "chrOffset": chrOffsets[chrom],
#             "importance": random.random(),
#             "fields": interval,
#         }
#         for interval in intervals2
#     ]

#     if not intervals:
#         return final_intervals

#     for interval in intervals:
#         try:
#             scores.append(int(interval[4]))
#         except (ValueError, IndexError):
#             scores.append(DEFAULT_SCORE)
#         intervals_length += 1

#     # generate beddb-like elements for parsing by the higlass plugin
#     if intervals_length >= min_elements and intervals_length <= max_elements:
#         for interval in intervals2:
#             try:
#                 score = int(interval[4])
#                 final_intervals.append(
#                     {
#                         "chrOffset": chrOffsets[chrom],
#                         "importance": score,
#                         "fields": interval,
#                     }
#                 )
#             except (ValueError, IndexError):
#                 final_intervals.append(
#                     {
#                         "chrOffset": chrOffsets[chrom],
#                         "importance": DEFAULT_SCORE,
#                         "fields": interval,
#                     }
#                 )

#     elif intervals_length > max_elements:
#         thresholded_intervals = []
#         desired_perc = max_elements / intervals_length
#         thresholded_score = int(np.quantile(scores, 1 - desired_perc))
#         for interval in intervals2:
#             try:
#                 score = int(interval[4])
#                 if score >= thresholded_score:
#                     thresholded_intervals.append(
#                         {
#                             "chrOffset": chrOffsets[chrom],
#                             "importance": score,
#                             "fields": interval,
#                         }
#                     )
#             except (ValueError, IndexError):
#                 if DEFAULT_SCORE >= thresholded_score:
#                     thresholded_intervals.append(
#                         {
#                             "chrOffset": chrOffsets[chrom],
#                             "importance": DEFAULT_SCORE,
#                             "fields": interval,
#                         }
#                     )
#         thresholded_intervals_length = len(thresholded_intervals)
#         if thresholded_intervals_length > max_elements:
#             indices = random.sample(range(thresholded_intervals_length), max_elements)
#             final_intervals = [thresholded_intervals[i] for i in sorted(indices)]

#     return final_intervals


def get_bigbed_tile(
    bbpath,
    zoom_level,
    start_pos,
    end_pos,
    chromsizes=None,
    range_mode=None,
    min_elements=None,
    max_elements=None,
):
    bbpath.seek(0)
    f = pybigtools.open(bbpath)

    if chromsizes is None:
        chromsizes = hgbw.get_chromsizes(bbpath)

    if min_elements is None:
        min_elements = MIN_ELEMENTS
    if max_elements is None:
        max_elements = MAX_ELEMENTS

    resolutions = hgbw.get_zoom_resolutions(chromsizes)
    binsize = resolutions[zoom_level]

    cids_starts_ends = list(hgbw.abs2genomic(chromsizes, start_pos, end_pos))
    results = []

    offset = 0
    offsetIdx = 0
    chrOffsets = {}
    for chrSize in chromsizes:
        chrOffsets[chromsizes.index[offsetIdx]] = offset
        offset += chrSize
        offsetIdx += 1

    intervals = []

    total_length = sum([c[2] - c[1] for c in cids_starts_ends])
    probs = [(c[2] - c[1]) / total_length for c in cids_starts_ends]

    # If there's a million chromosomes, pick at most 128 ones at random
    # weighted by their size
    NUM_TO_PICK = 128
    if NUM_TO_PICK < len(probs):
        rnds_ixs = nr.choice(
            len(cids_starts_ends), NUM_TO_PICK, p=probs, replace=NUM_TO_PICK
        )
        chosen_starts_ends = [cids_starts_ends[ix] for ix in rnds_ixs]
    else:
        chosen_starts_ends = cids_starts_ends

    for c in chosen_starts_ends:
        if c[0] >= len(chromsizes):
            continue
        # intervals += bbi.fetch_intervals(bbpath, chromsizes.index[c[0]], c[1], c[2])
        intervals += [
            # We're going to append the chromosome name to each record
            (chromsizes.index[c[0]],) + r
            for r in f.records(chromsizes.index[c[0]], c[1], c[2])
        ]

    MAX_RET = 100

    if len(intervals) > MAX_RET:
        chosen_intervals = random.choices(intervals, k=MAX_RET)
    else:
        chosen_intervals = intervals

    all_intervals = [
        {
            "chrOffset": chrOffsets[interval[0]],
            "importance": random.random(),
            "uid": md5("".join(map(str, interval)).encode('utf8')).hexdigest(),
            "fields": interval,
            'xStart': chrOffsets[interval[0]] + interval[1],
            'xEnd': chrOffsets[interval[0]] + interval[2],
        }
        for interval in chosen_intervals
    ]

    return all_intervals


def tiles(bbpath, tile_ids, chromsizes_map={}, chromsizes=None):
    """
    Generate tiles from a bigbed file.

    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved
    chromsizes_map: {uid: []}
        A set of chromsizes listings corresponding to the parameters of the
        tile_ids. To be used if a chromsizes id is passed in with the tile id
        with the `|cos:id` tag in the tile id
    chromsizes: [[chrom, size],...]
        A 2d array containing chromosome names and sizes. Overrides the
        chromsizes in chromsizes_map

    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    """

    min_elements = -1
    max_elements = -1

    generated_tiles = []
    for tile_id in tile_ids:
        tile_option_parts = tile_id.split(TILE_OPTIONS_CHAR)[1:]
        tile_no_options = tile_id.split(TILE_OPTIONS_CHAR)[0]
        tile_id_parts = tile_no_options.split(".")
        tile_position = list(map(int, tile_id_parts[1:3]))
        return_value = (
            tile_id_parts[3] if len(tile_id_parts) > 3 else DEFAULT_RANGE_MODE
        )

        range_mode = return_value if return_value in range_modes else None

        tile_options = dict([o.split(":") for o in tile_option_parts])

        if "min" in tile_options:
            min_elements = int(tile_options["min"])
        if "max" in tile_options:
            max_elements = int(tile_options["max"])

        if min_elements > max_elements:
            temp_min_elements = min_elements
            min_elements = max_elements
            max_elements = temp_min_elements
        elif min_elements == max_elements:
            min_elements = max_elements
            max_elements = min_elements + 1

        if max_elements <= 0:
            min_elements = MIN_ELEMENTS
            max_elements = MAX_ELEMENTS

        if chromsizes is not None:
            chromsizes_to_use = chromsizes
        else:
            chromsizes_id = None
            if "cos" in tile_options:
                chromsizes_id = tile_options["cos"]
            if chromsizes_id in chromsizes_map:
                chromsizes_to_use = chromsizes_map[chromsizes_id]
            else:
                chromsizes_to_use = None

        zoom_level = tile_position[0]
        tile_pos = tile_position[1]

        # this doesn't combine multiple consequetive ids, which
        # would speed things up
        if chromsizes_to_use is None:
            chromsizes_to_use = hgbw.get_chromsizes(bbpath)

        max_depth = hgbw.get_quadtree_depth(chromsizes_to_use)
        tile_size = hgbw.TILE_SIZE * 2 ** (max_depth - zoom_level)
        start_pos = tile_pos * tile_size
        end_pos = start_pos + tile_size

        tile_value = get_bigbed_tile(
            bbpath,
            zoom_level,
            start_pos,
            end_pos,
            chromsizes_to_use,
            range_mode=range_mode,
            min_elements=min_elements,
            max_elements=max_elements,
        )

        generated_tiles += [(tile_id, tile_value)]

    return generated_tiles


def chromsizes(filename):
    return hgbw.chromsizes(filename)
