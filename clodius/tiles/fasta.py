import math
from typing import Any, List, Tuple

import numpy as np

import clodius.tiles.chromsizes as cts
from clodius.tiles.format import format_dense_tile
from clodius.tiles.utils import TilesetInfo, abs2genome_fn, parse_tile_id

# from pysam import FastaFile

TILE_SIZE = 1024


def convert_bases_to_multivec(seq):
    res = []

    to_append = {
        "a": [1, 0, 0, 0, 0, 0],
        "t": [0, 1, 0, 0, 0, 0],
        "g": [0, 0, 1, 0, 0, 0],
        "c": [0, 0, 0, 1, 0, 0],
        "n": [0, 0, 0, 0, 1, 0],
    }

    for c in seq:
        res.append(to_append.get(c.lower(), [0, 0, 0, 0, 0, 1]))

    return res


def tileset_info(fai_filename):
    """
    Get the tileset info for a FASTA file

    Parameters
    ----------
    fai_filename: string
        The path to the FASTA index file from which to retrieve data
    chromsizes: [[chrom, size],...]
        A list of chromosome sizes associated with this tileset.
        Typically passed in to specify in what order data from
        the FASTA should be returned.

    Returns
    -------
    tileset_info: {'min_pos': [],
                    'max_pos': [],
                    'tile_size': 1024,
                    'max_zoom': 7
                    }
    """

    tsinfo = cts.tileset_info(fai_filename)
    tsinfo["max_zoom"] = math.ceil(
        math.log(tsinfo["max_pos"][0] / TILE_SIZE) / math.log(2)
    )

    tsinfo["max_width"] = TILE_SIZE * 2 ** tsinfo["max_zoom"]
    # tsinfo['bins_per_dimension'] = TILE_SIZE
    tsinfo["tile_size"] = TILE_SIZE
    tsinfo["datatype"] = "multivec_singleres_sequence"
    return tsinfo


def sequence_tiles_to_multivec(tiles):
    """Convert sequence tiles to multivec representation."""
    new_tiles = []
    for tile_id, tile in tiles:
        seq = tile["sequence"]
        res = convert_bases_to_multivec(seq)
        tile = format_dense_tile(np.array(res).T)
        tile["shape"] = [6, len(seq)]

        new_tiles += [(tile_id, tile)]
    return new_tiles


def multivec_tiles(*args, **kwargs):
    seq_tiles = sequence_tiles(*args, **kwargs)
    return sequence_tiles_to_multivec(seq_tiles)


def read_fai(fai_file):
    if isinstance(fai_file, str):
        fai_file = open(fai_file, "rb")

    fai_index = {}
    fai_file.seek(0)
    binary_data = fai_file.read()
    text_data = binary_data.decode("utf-8")

    for line in [l.strip() for l in text_data.split("\n") if l.strip()]:
        fields = line.strip().split("\t")
        seq_name = fields[0]
        seq_length = int(fields[1])
        offset = int(fields[2])
        line_blen = int(fields[3])
        line_len = int(fields[4])
        fai_index[seq_name] = (seq_length, offset, line_blen, line_len)
    return fai_index


def fetch_sequence(fasta_file, fai_index, seq_name, start, end):
    if isinstance(fasta_file, str):
        fasta_file = open(fasta_file, "rb")

    if seq_name not in fai_index:
        raise ValueError(f"Sequence {seq_name} not found in index")

    seq_length, offset, line_blen, line_len = fai_index[seq_name]

    if start < 0 or end > seq_length or start >= end:
        raise ValueError(f"Invalid range: {start}-{end} for sequence {seq_name}")

    # Calculate the byte range to read
    lines_to_skip = start // line_blen
    f = fasta_file
    # Move to the start of the sequence in the FASTA file
    f.seek(offset + lines_to_skip * line_len + (start % line_blen))

    # print("seq_name", seq_name)
    # print("line_blen", line_blen)
    # print("line_len", line_len)
    # print("start", start)
    # print("end", end)

    # Read the required lines
    total_read = 0
    sequence = []
    to_read = end - start
    while total_read < to_read:
        # print("end - start", end - start)
        chunk = f.read(min(end - start, line_blen - (start % line_blen)))
        # print("chunk", len(chunk))
        sequence.append(chunk.strip().decode("utf8"))
        start += len(chunk)
        total_read += len(chunk)
        f.seek(f.tell() + (line_len - line_blen))  # Skip to the next line

    full_seq = "".join(sequence)
    # print("len(full_seq):", len(full_seq))
    return full_seq


def sequence_tiles(
    fasta_filename: str,
    tile_ids: List[str],
    index_filename: str,
    chromsizes_fn: str = None,
) -> List[Tuple[str, Any]]:
    """Retrieve higlass tiles.

    Arguments:
        fasta_filename: The name of the fasta file to load
        tile_ids: The incoming tile ids (e.g. 'x.0.0')
        fai_filename: The name of the fasta index file (`samtools faidx`)
        chromsizes_filename: The chromsizes filename to use in case we
            want a specific chromosome order.
    Returns:
        Tile data
    """
    tsinfo = tileset_info(index_filename)
    tsinfo = TilesetInfo(**tsinfo)
    generated_tiles = []

    fa_index = read_fai(index_filename)

    if not chromsizes_fn:
        chromsizes_fn = index_filename

    for tile_id in tile_ids:
        tile_info = parse_tile_id(tile_id, tsinfo)

        zoom_diff = tsinfo.max_zoom - tile_info.zoom
        if zoom_diff > 3:
            generated_tiles += [
                (
                    tile_id,
                    {
                        "error": f"Tile too wide (zoom level {tile_info.zoom}). Please zoom in."
                    },
                )
            ]
            continue

        seq = ""

        for chr_interval in abs2genome_fn(
            chromsizes_fn, tile_info.start[0], tile_info.end[0]
        ):
            fs = fetch_sequence(
                fasta_filename,
                fa_index,
                chr_interval.name,
                chr_interval.start,
                chr_interval.end,
            )
            # fas = fa_file.fetch(chr_interval.name, chr_interval.start, chr_interval.end)

            # assert fs == fas

            seq += fs

        generated_tiles += [(tile_id, {"sequence": seq})]

    return generated_tiles
