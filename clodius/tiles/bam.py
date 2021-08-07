import json
import math

import numpy as np

import clodius.tiles.bigwig as ctbw
import pysam
from clodius.tiles.tabix import est_query_size_ix, load_bai_index
from clodius.tiles.utils import abs2genomic

from .utils import abs2genomic, natsorted


def get_cigar_substitutions(read):
    subs = []
    curr_pos = 0

    cigartuples = read.cigartuples
    readstart = read.pos
    readend = read.pos + read.query_length

    for ctuple in cigartuples:
        if ctuple[0] == pysam.CDIFF:
            subs.append((readstart + curr_pos, "X", ctuple[1]))
            curr_pos += ctuple[1]
        elif ctuple[0] == pysam.CINS:
            subs.append((readstart + curr_pos, "I", ctuple[1]))
        elif ctuple[0] == pysam.CDEL:
            subs.append((readstart + curr_pos, "D", ctuple[1]))
            curr_pos += ctuple[1]
        elif ctuple[0] == pysam.CREF_SKIP:
            subs.append((readstart + curr_pos, "N", ctuple[1]))
            curr_pos += ctuple[1]
        elif ctuple[0] == pysam.CEQUAL or ctuple[0] == pysam.CMATCH:
            curr_pos += ctuple[1]

    if len(cigartuples):
        first_ctuple = cigartuples[0]
        last_ctuple = cigartuples[-1]

        if first_ctuple[0] == pysam.CSOFT_CLIP:
            subs.append((readstart - first_ctuple[1], "S", first_ctuple[1]))
        if first_ctuple[0] == pysam.CHARD_CLIP:
            subs.append((readstart - first_ctuple[1], "H", first_ctuple[1]))

        if last_ctuple[0] == pysam.CSOFT_CLIP:
            subs.append((readend - last_ctuple[1], "S", last_ctuple[1]))
        if last_ctuple[0] == pysam.CHARD_CLIP:
            subs.append((readend, "H", last_ctuple[1]))

    return subs


def load_reads(
    samfile, start_pos, end_pos, chromsizes=None, index_filename=None, cache=None
):
    """
    Sample reads from the specified region, assuming that the chromosomes
    are ordered in some fashion. Returns an list of pysam reads

    Parameters:
    -----------
    samfile: pysam.AlignmentFile
        A pysam entry into an indexed bam file
    start_pos: int
        The start position of the sampled region
    end_pos: int
        The end position of the sampled region
    chromsize: pandas.Series
        A listing of chromosome sizes. If not provided, the chromosome
        list will be extracted from the the bam file header
    cache: 
        An object that implements the `get`, `set` and `exists` methods
        for caching data

    Returns
    -------
    reads: [read1, read2...]
        The list of in the sampled regions
    """
    # if chromorder is not None...
    # specify the chromosome order for the fetched reads

    if chromsizes is not None:
        chromsizes_list = []

        for chrom, size in chromsizes.iteritems():
            chromsizes_list += [[chrom, int(size)]]
    else:
        references = np.array(samfile.references)
        lengths = np.array(samfile.lengths)

        ref_lengths = dict(zip(references, lengths))

        # we're going to create a natural ordering for references
        # e.g. (chr1, chr2,..., chr10, chr11...chr22,chrX, chrY, chrM...)
        references = ctbw.natsorted(references)
        chromsizes_list = list(zip(references, [int(length) for length in lengths]))

    lengths = [r[1] for r in chromsizes_list]
    abs_chrom_offsets = np.r_[0, np.cumsum(lengths)]

    results = {
        "id": [],
        "from": [],
        "to": [],
        "md": [],
        "chrName": [],
        "chrOffset": [],
        "cigar": [],
        "m1From": [],
        "m1To": [],
        "m2From": [],
        "m2To": [],
        "mapq": [],
        "tags.HP": [],
        "strand": [],
        "variants": [],
        "cigars": [],
    }

    strands = {True: "-", False: "+"}
    import time

    idx = load_bai_index(index_filename)

    total_size = 0
    # check the size of the file to load to get an approximation
    # of whether we're going to return too much data
    for cid, start, end in abs2genomic(lengths, start_pos, end_pos):
        total_size += est_query_size_ix(idx[cid], start, end)

    MAX_SIZE = 4e6
    if total_size > MAX_SIZE:
        return {"error": "Tile encompasses too much data: {total_size}"}

    for cid, start, end in abs2genomic(lengths, start_pos, end_pos):
        chr_offset = int(abs_chrom_offsets[cid])

        if cid >= len(chromsizes_list):
            continue

        seq_name = f"{chromsizes_list[cid][0]}"
        reads = samfile.fetch(seq_name, start, end)
        for read in reads:
            if read.is_unmapped:
                continue
            # query_seq = read.query_sequence

            # differences = []

            # try:
            #     for counter, (qpos, rpos, ref_base) in enumerate(read.get_aligned_pairs(with_seq=True)):
            #         # inferred from the pysam source code:
            #         # https://github.com/pysam-developers/pysam/blob/3defba98911d99abf8c14a483e979431f069a9d2/pysam/libcalignedsegment.pyx
            #         # and GitHub issue:
            #         # https://github.com/pysam-developers/pysam/issues/163
            #         #print('qpos, rpos, ref_base', qpos, rpos, ref_base)
            #         if rpos is None:
            #             differences += [(qpos, 'I')]
            #         elif qpos is None:
            #             differences += [(counter, 'D')]
            #         elif ref_base.islower():
            #             differences += [(qpos, query_seq[qpos], ref_base)]
            # except ValueError as ve:
            #     # probably lacked an MD string
            #     pass
            try:
                id_suffix = ""
                if read.is_paired:
                    if read.is_read1:
                        id_suffix = "_1"
                    if read.is_read2:
                        id_suffix = "_2"

                read_id = read.query_name + id_suffix
                results["id"] += [read_id]
                results["from"] += [int(read.reference_start + chr_offset)]
                results["to"] += [int(read.reference_end + chr_offset)]
                results["chrName"] += [read.reference_name]
                results["chrOffset"] += [chr_offset]
                results["cigar"] += [read.cigarstring]
                results["mapq"] += [read.mapq]
                # aligned_pairs = read.get_aligned_pairs(with_seq=True)

                # For ONT reads retrieving the variants can be a lengthy
                # procedure. We can try to cache them
                use_cache = read.query_length > 40000
                if use_cache:
                    variants = get_cached_variants(cache, read_id)
                else:
                    variants = None
                # variants = None

                if not variants:
                    if read.query_sequence:
                        # read.get_aligned_pairs(with_seq=True, matches_only=True)
                        try:
                            variants = [
                                (r[0], r[1], read.query_sequence[r[0]])
                                for r in read.get_aligned_pairs(
                                    with_seq=True, matches_only=True
                                )
                                if start <= r[1] <= end
                                and r[2] is not None
                                and r[2].islower()
                            ]
                        except ValueError:
                            # Probably MD tag not present
                            variants = []

                        if use_cache:
                            set_cached_variants(cache, read_id, variants)

                        results["variants"] += [variants]
                    else:
                        results["variants"] += []
                else:
                    results["variants"] += [variants]

                results["cigars"] += [get_cigar_substitutions(read)]
                tags = dict(read.tags)
                results["tags.HP"] += [tags.get("HP", 0)]
                results["strand"] += [strands[read.is_reverse]]
            except:
                raise

            try:
                results["md"] += [read.get_tag("MD")]
            except KeyError:
                results["md"] += [""]
                continue

    return results


def get_cached_variants(cache, read_id):
    """Try to get variants from a read we've seen before.

    This is useful for ONT reads where there's many variants
    per read and retrieving them takes a while.
    """
    cache_id = f"variants.{read_id}"
    if cache and cache.exists(cache_id):
        return json.loads(cache.get(cache_id))

    return None


def set_cached_variants(cache, read_id, variants):
    """Save a set of variants to the cache."""
    cache_id = f"variants.{read_id}"
    if cache:
        cache.set(cache_id, json.dumps(variants))


def alignment_tileset_info(samfile, chromsizes):
    """
    Get the tileset info for a bam file

    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from

    Returns
    -------
    tileset_info: {'min_pos': [],
                    'max_pos': [],
                    'tile_size': 1024,
                    'max_zoom': 7
                    }
    """
    if chromsizes is not None:
        chromsizes_list = []

        for chrom, size in chromsizes.iteritems():
            chromsizes_list += [[chrom, int(size)]]

        total_length = sum([c[1] for c in chromsizes_list])
    else:
        total_length = sum(samfile.lengths)

        references = np.array(samfile.references)
        lengths = np.array(samfile.lengths)

        ref_lengths = dict(zip(references, lengths))
        references = ctbw.natsorted(references)

        lengths = [ref_lengths[r] for r in references]
        chromsizes_list = list(zip(references, [int(length) for length in lengths]))

    tile_size = 256
    max_zoom = math.ceil(math.log(total_length / tile_size) / math.log(2))

    # this should eventually be a configurable option
    MAX_TILE_WIDTH = 100000

    tileset_info = {
        "min_pos": [0],
        "max_pos": [total_length],
        "max_width": tile_size * 2 ** max_zoom,
        "tile_size": tile_size,
        "chromsizes": chromsizes_list,
        "max_zoom": max_zoom,
        "max_tile_width": MAX_TILE_WIDTH,
    }

    return tileset_info


def alignment_tiles(
    samfile,
    tile_ids,
    index_filename=None,
    chromsizes=None,
    max_tile_width=None,
    cache=None,
):
    """
    Generate tiles from a bigwig file.

    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved
    index_filename: str
        The name of the file containing the index
    max_tile_width: int
        How wide can each tile be before we return no data. This
        can be used to limit the amount of data returned.
    cache: 
        An object that implements the `get`, `set` and `exists` methods
        for caching data
    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    """
    generated_tiles = []
    tsinfo = alignment_tileset_info(samfile, chromsizes)

    for tile_id in tile_ids:
        tile_id_parts = tile_id.split("|")[0].split(".")
        tile_position = list(map(int, tile_id_parts[1:3]))

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
        else:
            start_pos = int(tile_position[1]) * tile_width
            end_pos = start_pos + tile_width

            tile_value = load_reads(
                samfile,
                start_pos=start_pos,
                end_pos=end_pos,
                chromsizes=chromsizes,
                index_filename=index_filename,
                cache=cache,
            )
            generated_tiles += [(tile_id, tile_value)]

    return generated_tiles


def tileset_info(filename, chromsizes=None):
    samfile = pysam.AlignmentFile(filename)

    return alignment_tileset_info(samfile, chromsizes)


def tiles(
    filename,
    tile_ids,
    index_filename=None,
    chromsizes=None,
    max_tile_width=None,
    cache=None,
):
    if not index_filename:
        index_filename = f"{filename}.bai"
    samfile = pysam.AlignmentFile(filename, index_filename=index_filename)

    return alignment_tiles(
        samfile,
        tile_ids,
        index_filename=index_filename,
        chromsizes=chromsizes,
        max_tile_width=None,
        cache=cache,
    )
