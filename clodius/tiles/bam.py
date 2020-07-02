import math
import numpy as np
import pysam
import clodius.tiles.bigwig as ctbw


def abs2genomic(chromsizes, start_pos, end_pos):
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
    yield cid_hi, start, rel_pos_hi


def load_reads(samfile, start_pos, end_pos, chrom_order=None):
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
    chrom_order: ['chr1', 'chr2',...]
        A listing of chromosome names to use as the order

    Returns
    -------
    reads: [read1, read2...]
        The list of in the sampled regions
    """
    # if chromorder is not None...
    # specify the chromosome order for the fetched reads

    references = np.array(samfile.references)
    lengths = np.array(samfile.lengths)

    ref_lengths = dict(zip(references, lengths))

    # we're going to create a natural ordering for references
    # e.g. (chr1, chr2,..., chr10, chr11...chr22,chrX, chrY, chrM...)
    references = ctbw.natsorted(references)
    lengths = [ref_lengths[r] for r in references]

    abs_chrom_offsets = np.r_[0, np.cumsum(lengths)]

    if chrom_order:
        chrom_order = np.array(chrom_order)
        chrom_order_ixs = np.nonzero(np.in1d(references, chrom_order))
        lengths = lengths[chrom_order_ixs]

    results = {
        "id": [],
        "from": [],
        "to": [],
        "md": [],
        "chrName": [],
        "chrOffset": [],
        "cigar": [],
    }

    for cid, start, end in abs2genomic(lengths, start_pos, end_pos):
        chr_offset = int(abs_chrom_offsets[cid])

        if cid >= len(references):
            continue

        seq_name = f"{references[cid]}"
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
                results["id"] += [read.query_name]
                results["from"] += [int(read.reference_start + chr_offset)]
                results["to"] += [int(read.reference_end + chr_offset)]
                results["chrName"] += [read.reference_name]
                results["chrOffset"] += [chr_offset]
                results["cigar"] += [read.cigarstring]
            except:
                raise

            try:
                results["md"] += [read.get_tag("MD")]
            except KeyError:
                results["md"] += [""]
                continue

    return results


def tileset_info(filename):
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
    samfile = pysam.AlignmentFile(filename)
    total_length = sum(samfile.lengths)

    references = np.array(samfile.references)
    lengths = np.array(samfile.lengths)

    ref_lengths = dict(zip(references, lengths))
    lengths = [ref_lengths[r] for r in references]

    tile_size = 256
    max_zoom = math.ceil(math.log(total_length / tile_size) / math.log(2))

    tileset_info = {
        "min_pos": [0],
        "max_pos": [total_length],
        "max_width": tile_size * 2 ** max_zoom,
        "tile_size": tile_size,
        "chromsizes": list(zip(references, map(int, lengths))),
        "max_zoom": max_zoom,
    }

    return tileset_info


def tiles(filename, tile_ids, index_filename=None, max_tile_width=None):
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

    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    """
    generated_tiles = []
    tsinfo = tileset_info(filename)
    samfile = pysam.AlignmentFile(filename, index_filename=index_filename)

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

            tile_value = load_reads(samfile, start_pos=start_pos, end_pos=end_pos)
            generated_tiles += [(tile_id, tile_value)]

    return generated_tiles
