import collections as col
import gzip
import struct
import polars as pl
import pandas as pd
from typing import Literal

import numpy as np
from smart_open import open

from clodius.tiles.bigwig import abs2genomic
from clodius.utils import get_file_compression


def load_bai_index(index_file):
    """Load a reduced version of a bai index so that we can
    go through it and get a sense of how much data will be
    retrieved by a query."""
    f = index_file
    b = bytearray(f.read())

    [_, _, _, _, n_ref] = struct.unpack("<4cI", b[:8])
    c = 8

    indeces = []

    for i in range(n_ref):
        n_bin = struct.unpack("<I", b[c : c + 4])[0]
        c += 4
        bins = col.defaultdict(list)
        for j in range(n_bin):
            [bin_no, n_chunk] = struct.unpack("<II", b[c : c + 8])
            c += 8

            bytes_to_read = n_chunk * 2 * 8
            unpack_str = f"<{2 * n_chunk}Q"
            bins[bin_no] = struct.unpack(unpack_str, b[c : c + bytes_to_read])
            c += bytes_to_read

        n_intv = struct.unpack("<I", b[c : c + 4])[0]
        c += 4 + 8 * n_intv

        indeces += [bins]

    return indeces


def load_tbi_idx(index_filename):
    """Load a reduced version of a tabix index so that we can
    go through it and get a sense of how much data will be
    retrieved by a query."""
    if isinstance(index_filename, str):
        f = open(index_filename, "rb")
    else:
        f = index_filename

    f.seek(0)

    with gzip.GzipFile(fileobj=f, mode="rb") as gz:
        b = gz.read()

    [
        _,
        _,
        _,
        _,
        n_ref,
        format,
        col_seq,
        col_beg,
        col_end,
        meta,
        skip,
        l_nm,
    ] = struct.unpack("<4ciiiiiiii", b[:36])
    c = 36
    names = [n.decode("ascii") for n in b[c : c + l_nm].split(b"\0")]
    c += l_nm

    indeces = []

    for i in range(n_ref):
        n_bin = struct.unpack("<i", b[c : c + 4])[0]
        c += 4
        bins = col.defaultdict(list)
        for j in range(n_bin):
            [bin_no, n_chunk] = struct.unpack("<Ii", b[c : c + 8])
            c += 8

            bytes_to_read = n_chunk * 2 * 8
            unpack_str = f"<{2 * n_chunk}Q"
            bins[bin_no] = struct.unpack(unpack_str, b[c : c + bytes_to_read])
            c += bytes_to_read

        n_intv = struct.unpack("<i", b[c : c + 4])[0]
        c += 4 + 8 * n_intv

        indeces += [bins]

    return dict(zip(names, indeces))


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def reg2bins(begin, end, n_lvls=5, min_shift=14):
    """
    generate key of bins which may overlap the given region,
    check out https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3042176/
    and https://samtools.github.io/hts-specs/tabix.pdf
    for more information.
    Parameters
    ----------
    begin: int
        chromosome position begin
    end: int
        chromosome position end
    n_lvls: int, optional
        cluster level, for tabix, set to 5
    min_shift: int, optional
        minimum shift, for tabix, set to 14
    Returns
    -------
    generator
    """
    begin, end = begin, end
    t, s = 0, min_shift + (n_lvls << 1) + n_lvls
    for l in range(n_lvls + 1):
        b, e = t + (begin >> s), t + (end >> s)
        n = e - b + 1
        for k in range(b, e + 1):
            yield k
            n += 1
        t += 1 << ((l << 1) + l)
        s -= 3


def est_query_size_ix(ix, start, end):
    total_size = 0

    for bin in list(reg2bins(start, end)):
        if 4681 <= bin <= 37448:
            # only consider the lowest level bins
            if ix[bin]:
                bin_size = 0
                for chunk in chunks(ix[bin], 2):
                    bin_size += (chunk[1] >> 16) - (chunk[0] >> 16)
                    total_size += (chunk[1] >> 16) - (chunk[0] >> 16)
                #             print(bin, chunk, ix[bin], (chunk[1] >> 16) - (chunk[0] >> 16))
                # print(bin, bin_size)
    return total_size


def est_query_size(index, name, start, end):
    if name not in index:
        return 0

    ix = index[name]
    return est_query_size_ix(ix, start, end)


def dataframe_tabix_fetcher(file, index, ref, start, end):
    """Fetch rows of a tabix indexed file into a dataframe."""
    import oxbow as ox

    if isinstance(index, str):
        index = open(index, "rb", compression="disable")

    if start == 0:
        start = 1
    pos = f"{ref}:{start}-{end}"

    file.seek(0)
    index.seek(0)

    try:
        arrow_ipc = ox.read_tabix(file, pos, index)
    except ValueError as ex:
        if "missing reference sequence" in str(ex):
            return None
        raise

    df = pl.read_ipc(arrow_ipc)
    return df


def single_indexed_tile(
    file,
    index,
    chromsizes,
    tsinfo,
    z,
    x,
    tbx_index,
    fetcher=dataframe_tabix_fetcher,
    max_tile_width=None,
    max_results=None,
):
    tile_width = tsinfo["max_width"] / 2**z

    if max_tile_width and tile_width > max_tile_width:
        raise ValueError(f"Tile too wide {tile_width}. Max width: {max_tile_width}.")

    query_size = 0

    start_pos = x * tsinfo["max_width"] / 2**z
    end_pos = (x + 1) * tsinfo["max_width"] / 2**z

    cids_starts_ends = list(abs2genomic(chromsizes, start_pos, end_pos))
    ret_vals = None

    if tbx_index:
        for cid, start, end in cids_starts_ends:
            if cid >= len(chromsizes):
                continue

            chrom = chromsizes.index[cid]

            query_size += est_query_size(tbx_index, chrom, int(start), int(end))

    MAX_QUERY_SIZE = 1000000

    if query_size > MAX_QUERY_SIZE:
        raise ValueError(f"Tile too large {query_size}")

    for cid, start, end in cids_starts_ends:
        if cid >= len(chromsizes):
            continue

        chrom = chromsizes.index[cid]
        df = fetcher(file, index, str(chrom), int(start), int(end))
        if df is not None:
            if ret_vals is None:
                ret_vals = df
            else:
                ret_vals = pl.concat([ret_vals, df])

    if max_results and len(ret_vals) > max_results:
        raise ValueError(f"Too many values in tile {len(ret_vals)}")

    return ret_vals


def df_single_tile(filename, chromsizes, tsinfo, z, x, mode: Literal["gff", "bed"]):
    """Load a single tile from the filename."""
    tile_width = tsinfo["max_width"] / 2**z
    start_pos = x * tile_width
    end_pos = (x + 1) * tile_width

    cids_starts_ends = list(abs2genomic(chromsizes, start_pos, end_pos))

    # Reset file position to beginning if it's a file object
    if hasattr(filename, "seek"):
        filename.seek(0)

    df = pl.from_pandas(
        pd.read_csv(
            filename,
            delimiter="\t",
            header=None,
            comment="#",
            compression=get_file_compression(filename),
        )
    )

    if mode == "gff":
        df.columns = [
            "seqid",
            "source",
            "type",
            "start",
            "end",
            "score",
            "strand",
            "phase",
            "attributes",
        ]

    filtered_rows = []

    for cid, tile_start, tile_end in cids_starts_ends:
        if cid >= len(chromsizes):
            continue

        chrom = chromsizes.index[cid]

        if mode == "gff":
            chrom_col, start_col, end_col = "seqid", "start", "end"
        else:  # bed
            chrom_col, start_col, end_col = "column_1", "column_2", "column_3"

        mask = (
            (df[chrom_col] == chrom)
            & (df[end_col] > tile_start)
            & (df[start_col] < tile_end)
        )

        filtered_rows.append(df.filter(mask))

    if filtered_rows:
        return pl.concat(filtered_rows)
    else:
        return pl.DataFrame()
