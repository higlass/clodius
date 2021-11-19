from pyfaidx import Fasta
import numpy as np
import pandas as pd
import logging
from .utils import abs2genomic, natsorted, get_quadtree_depth

logger = logging.getLogger(__name__)

TILE_SIZE = 1024


def get_chromsizes(fapath):
    with Fasta(fapath, one_based_attributes=False) as fa:
        chromsizes = dict((seq, len(fa.records[seq])) for seq in fa.keys())
        chromosomes = natsorted(fa.keys())
    return pd.Series(chromsizes)[chromosomes]


def tileset_info(fapath, chromsizes=None):
    """
    Get the tileset info for a FASTA file

    Parameters
    ----------
    fapath: string
        The path to the FASTA file from which to retrieve data
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
    if chromsizes is None:
        chromsizes = get_chromsizes(fapath)
        chromsizes_list = []

        for chrom, size in chromsizes.iteritems():
            chromsizes_list += [[chrom, int(size)]]
    else:
        chromsizes_list = chromsizes
        chromsizes = [int(c[1]) for c in chromsizes_list]
    max_zoom = get_quadtree_depth(chromsizes, TILE_SIZE)
    tileset_info = {
        "min_pos": [0],
        "max_pos": [sum(chromsizes)],
        "max_width": TILE_SIZE * 2 ** max_zoom,
        "tile_size": TILE_SIZE,
        "max_zoom": max_zoom,
        "chromsizes": chromsizes_list,
    }
    return tileset_info


def abs2genomic(chromsizes, start_pos, end_pos):
    """
    Convert absolute genomic sizes to genomic

    Parameters:
    -----------
    chromsizes: [1000,...]
        An array of the lengths of the chromosomes
    start_pos: int
        The starting genomic position
    end_pos: int
        The ending genomic position
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
    yield cid_hi, start, rel_pos_hi


def get_fasta_tile(
    fapath, zoom_level, start_pos, end_pos, chromsizes=None,
):
    if chromsizes is None:
        chromsizes = get_chromsizes(fapath)
    chrom_names = chromsizes.keys()
    cids_starts_ends = list(abs2genomic(chromsizes, start_pos, end_pos))
    with Fasta(fapath, one_based_attributes=False) as fa:
        # investigate using 4 bits per character (only 16 possible chars)
        arrays = [
            fa[chrom_names[cid]][start:end].seq for cid, start, end in cids_starts_ends
        ]
    return "".join(arrays)


def tiles(fapath, tile_ids, chromsizes_map={}, chromsizes=None, max_tile_width=None):
    """
    Generate tiles from a FASTA file.

    Parameters
    ----------
    fapath: str
        The filepath of the FASTA file
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
    max_tile_width: int
        How wide can each tile be before we return no data. This
        can be used to limit the amount of data returned.
    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    """
    generated_tiles = []
    for tile_id in tile_ids:
        tile_option_parts = tile_id.split("|")[1:]
        tile_no_options = tile_id.split("|")[0]
        tile_id_parts = tile_no_options.split(".")
        tile_position = list(map(int, tile_id_parts[1:3]))

        tile_options = dict([o.split(":") for o in tile_option_parts])

        if chromsizes:
            chromnames = [c[0] for c in chromsizes]
            chromlengths = [int(c[1]) for c in chromsizes]
            chromsizes_to_use = pd.Series(chromlengths, index=chromnames)
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
            chromsizes_to_use = get_chromsizes(fapath)

        max_depth = get_quadtree_depth(chromsizes_to_use, TILE_SIZE)
        tile_size = TILE_SIZE * 2 ** (max_depth - zoom_level)
        if max_tile_width and tile_size > max_tile_width:
            return [
                (
                    tile_id,
                    {
                        "error": f"Tile too large, no data returned. Max tile size: {max_tile_width}"
                    },
                )
            ]
        start_pos = tile_pos * tile_size
        end_pos = start_pos + tile_size
        tile = get_fasta_tile(fapath, zoom_level, start_pos, end_pos, chromsizes_to_use)
        generated_tiles += [(tile_id, {"sequence": tile})]
    return generated_tiles


def chromsizes(filename):
    """
    Get a list of chromosome sizes from this [presumably] fasta
    file.

    Parameters:
    -----------
    filename: string
        The filename of the fasta file

    Returns
    -------
    chromsizes: [(name:string, size:int), ...]
        An ordered list of chromosome names and sizes
    """
    try:
        chrom_series = get_chromsizes(filename)
        data = []
        for chrom, size in chrom_series.iteritems():
            data.append([chrom, size])
        return data
    except Exception as ex:
        logger.error(ex)
        raise Exception("Error loading chromsizes from bigwig file: {}".format(ex))
