from clodius.alignment import (
    generate_pwm_from_sequences,
    DNA_ALPHABET,
    PROTEIN_ALPHABET,
)
from typing import Literal
from clodius.tiles import npvector
from clodius.tiles.csv import csv_sequence_tileset_functions
import numpy as np
import base64
from functools import lru_cache
from typing import Optional


def tile_functions(
    sequences,
    seqtype: Optional[Literal["dna", "protein"]] = None,
    refseq=None,
    **kwargs,
):
    pwm, seqs = generate_pwm_from_sequences(sequences, seqtype=seqtype, refseq=refseq)

    if seqtype is None:
        seqtype = "dna" if len(pwm) == 4 else "protein"
    if seqtype == "dna":
        alphabet = DNA_ALPHABET
    elif seqtype == "protein":
        alphabet = PROTEIN_ALPHABET
    else:
        raise ValueError(f"Unknown type: {type}. Expected 'dna'.")

    vector = np.array([pwm[b] for b in alphabet])

    bin_size = 512
    tsinfo = npvector.tileset_info(vector, bins_per_dimension=bin_size)

    tsinfo["shape"] = [vector.shape[0], bin_size]
    tsinfo["row_infos"] = alphabet
    tsinfo["resolutions"] = sorted(
        [2**i for i in range(tsinfo["max_zoom"] + 1)], key=lambda x: -x
    )
    tsinfo["aligned_seqs"] = seqs
    # tsinfo["max_pos"] = len(vector[0])

    del tsinfo["max_zoom"]
    del tsinfo["max_width"]

    def tileset_info():
        return tsinfo

    def tiles(tile_ids):
        to_ret = []

        for tile_id in tile_ids:
            parts = tile_id.split(".")
            z = int(parts[1])
            x = int(parts[2])

            t = npvector.tiles(vector.T, z, x, bin_size=bin_size)
            dense = t.T.ravel().astype("float16")
            d = base64.b64encode(np.array(dense, dtype="float16")).decode("utf-8")

            to_ret += (
                (
                    tile_id,
                    {
                        # "dense": "ozhEMog68jvvNGw1+jrfNUU66i0ULDE4EjcAOcs7nDldO8I6CjvuO7wy/DgbOrQ7PTqtOYA0FjjNN+IyFDLhMs44WTTwNDU0ezTXK803pzgZOXA3sDn3NY86vzUVON43+TcmMfs1kzvoMMYsejYfNjI5hjCNOW86STjPLnE7cDeDM/k2QC+xOtA2ZTUeOJs5yyx9Mb4uRTW6LFMxlTc5Ovo4azqfOTE5YzPANyM5/TuXLTI50zchO407SDL7OFA7/Ti0OpwuczK4Njw6UjlPJRY7/zkdMNc4fTVYOjEpayb1MkY6BjglNfI7CDbQIJEcrSwcOLU1azrXOUw5ZjUcOiowdDmKOQI4nDVoO4IsBTleOm0xbTVlNoM1DDsbOcI7wDvSOcs31y7VOLwzizovM2IzPCyoHvkrVjc/ODM0CTM=",
                        "dense": d,
                        "dtype": "float16",
                        "shape": [vector.shape[0], bin_size],
                    },
                ),
            )
        return to_ret

    return {"tileset_info": tileset_info, "tiles": tiles}


def get_local_tiles(filename, colname=None, colnum=None, sep=","):
    """Get local higlass tiles for the provided file."""
    tsinfo = csv_tileset_info(filename, colname=colname, colnum=colnum, sep=sep)
    max_resolution = max(tsinfo["resolutions"])

    tile_ids = []

    for i, res in enumerate(sorted(tsinfo["resolutions"], key=lambda x: -x)):
        print("res", i, res)
        for j in range(0, max_resolution // res):
            tile_ids += [f"x.{i}.{j}"]

    tiles = dict(csv_tiles(filename, tile_ids, colname=colname, colnum=colnum, sep=sep))

    return {"tilesetInfo": {"x": tsinfo}, "tiles": tiles}


def csv_tileset_info(filename, *csv_args, **csv_kwargs):
    """Get tileset info for a sequence logo file file from
    a csv file.

    Parameters
    ----------
    filename: string
        The name of the csv file
    colname: Optional[str]
        The name of the column containing the sequences.
    colnum: Optional[int]
        The column number of the sequence logo file. 0-based.
        Only used if colname is not provided.
    header: bool
        Whether to assume that a header is present in the csv file
    sep: string
        The separator used in the csv file
    refrow: A row to use as a reference sequence when calculating
        alignments. Should be 1-based
    """
    tf = csv_sequence_tileset_functions(
        filename, tile_functions=tile_functions, *csv_args, **csv_kwargs
    )
    return tf["tileset_info"]()


def csv_tiles(filename, tile_ids, *csv_args, **csv_kwargs):
    tf = csv_sequence_tileset_functions(
        filename, tile_functions=tile_functions, *csv_args, **csv_kwargs
    )

    return tf["tiles"](tile_ids)
