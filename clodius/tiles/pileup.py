from Bio import Align
import tempfile
from clodius.alignment import align_sequences, alignment_to_subs, order_by_clustering
from clodius.tiles.csv import csv_sequence_tileset_functions
from clodius.tiles.bam import parse_cigar_string, get_cigar_substitutions


def get_subs(alignment):
    """Wrapper for alignment_to_subs that returns the result."""
    return alignment_to_subs(alignment)


def get_pileup_alignment_data(refseq, seqs, cluster=None, values=None):
    """Get pileup alignment data for a reference sequence and a list of sequences."""
    chromsizes = [("ref", len(refseq))]
    refseqs = [{"id": "ref", "seq": refseq}]

    tf = tile_functions(
        seqs, refseqs, cluster=cluster, values=values, chromsizes=chromsizes
    )
    tsinfo = tf["tileset_info"]()
    tiles = tf["tiles"](["0.0"])

    return {"type": tsinfo, "tiles": dict(tiles)}


def calc_chr_offset(chromsizes, chrom_id):
    sum = 0
    for chrom in chromsizes:
        if chrom[0] == chrom_id:
            return sum
        sum += chrom[1]


def get_substitutions(hit, seq):
    """
    :param hit: mappy.Alignment object (result of a.map())
    :param seq: The query sequence string
    """
    substitutions = []

    # mappy provides hit.cs (difference string)
    # Format: :[len] (match), *[ref][query] (substitution), +[seq] (insertion), -[seq] (deletion)
    # Example: :10*at:5+cc:2

    curr_pos = 0  # Position relative to target start (hit.ts)
    read_pos = 0  # Position relative to read start (including soft clipping)

    # 1. Handle Leading Soft Clipping
    # mappy.Alignment.cigar is a list of (length, op)
    if hit.cigar[0][1] == 4:
        sc_len = hit.cigar[0][0]
        substitutions.append(
            {"pos": -sc_len, "length": sc_len, "type": "S", "variant": seq[:sc_len]}
        )
        read_pos += sc_len

    # 2. Parse the CS tag for Mismatches, Inserts, and Deletes
    # We use regex to split the CS tag into its components
    import re

    cs_parts = re.findall(r"(:[0-9]+|\*[a-z][a-z]|\+[a-z]+|-[a-z]+)", hit.cs)

    for part in cs_parts:
        op = part[0]

        if op == ":":  # Match
            ln = int(part[1:])
            curr_pos += ln
            read_pos += ln

        elif op == "*":  # Substitution (Mismatch)
            # val is 'ag' meaning ref was 'a', read is 'g'
            ref_base = part[1].upper()  # The first char is the REF
            query_base = part[2].upper()  # The second char is the QUERY
            substitutions.append(
                {
                    "pos": curr_pos,
                    "length": 1,
                    "type": "X",
                    "base": ref_base,  # Original base
                    "variant": query_base,  # Mismatched base
                }
            )
            curr_pos += 1
            read_pos += 1

        elif op == "+":  # Insertion
            val = part[1:].upper()
            ins_len = len(val)
            substitutions.append(
                {
                    "pos": curr_pos,
                    "length": ins_len,
                    "type": "I",
                    "base": "",
                    "variant": val.upper(),
                }
            )
            read_pos += ins_len

        elif op == "-":  # Deletion
            val = part[1:].upper()
            substitutions.append(
                {
                    "pos": curr_pos,
                    "length": len(val),
                    "type": "D",
                    "base": val,  # Original bases that were deleted
                    "variant": "",  # No variant base in query
                }
            )
            curr_pos += len(val)

    # 3. Handle Trailing Soft Clipping
    if hit.cigar[-1][1] == 4:
        sc_len = hit.cigar[-1][0]
        substitutions.append(
            {
                "pos": hit.te - hit.ts,
                "length": sc_len,
                "type": "S",
                "variant": seq[-sc_len:],
            }
        )

    return substitutions


def align_sequences(seq1, seq2):
    """Align two sequences to each other and return an alignment object."""
    aligner = Align.PairwiseAligner()

    aligner.match_score = 1
    aligner.mismatch_score = -4
    aligner.open_gap_score = -6
    aligner.extend_gap_score = -1

    alignments = aligner.align(seq1, seq2)

    best_alignment = alignments[0]

    return best_alignment


def tile_functions(seqs, refseqs, cluster=None, values=None, chromsizes=None):
    """Return a dictionary of tile functions for the pileup track."""
    longest_seq = sum([c[1] for c in chromsizes])

    def tileset_info():
        return {
            "tile_size": longest_seq,
            "resolutions": [1],
            "max_tile_width": longest_seq,
            "format": "subs",
            "min_pos": [0],
            "max_pos": [longest_seq],
            "chromsizes": chromsizes,
        }

    if cluster == "linkage":
        seqs = order_by_clustering(seqs)

    tile = []
    for i, seq in enumerate(seqs):
        for refseq in refseqs:
            a = align_sequences(refseq["seq"], seq)
            start, end, subs = alignment_to_subs(a)

            chr_offset = calc_chr_offset(chromsizes, refseq["id"])

            tv = {
                "id": f"r{i}_{refseq['id']}",
                "from": start + chr_offset,
                "to": end + chr_offset,
                "substitutions": subs,
                "color": 0,
            }

            if values:
                tv["extra"] = values[i]

            tile.append(tv)

    def tiles(tile_ids):
        tiles = []

        for tile_id in tile_ids:
            parts = tile_id.split(".")
            z = int(parts[1])
            x = int(parts[2])

            if z != 0 and x != 0:
                # return an empty tile
                tiles += [(tile_id, [])]
            else:
                # return the entire tile
                tiles += [(tile_id, tile)]

        return tiles

    return {"tileset_info": tileset_info, "tiles": tiles}


def tile_functions_fasta(seqs, refseqs, cluster=None, values=None, chromsizes=None):
    """Return a dictionary of tile functions for the pileup track using FASTA and mappy."""
    import mappy as mp

    longest_seq = sum([c[1] for c in chromsizes])

    def tileset_info():
        return {
            "tile_size": longest_seq,
            "resolutions": [1],
            "max_tile_width": longest_seq,
            "format": "subs",
            "min_pos": [0],
            "max_pos": [longest_seq],
            "chromsizes": chromsizes,
        }

    if cluster == "linkage":
        seqs = order_by_clustering(seqs)

    # Write refseqs to temp file in FASTA format
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".fasta", dir="/tmp", delete=False
    ) as tmp_file:
        for refseq in refseqs:
            tmp_file.write(f">{refseq['id']}\n{refseq['seq']}\n")
        tmp_filename = tmp_file.name

    # Create mappy aligner from temp file
    aligner = mp.Aligner(tmp_filename, preset="sr")

    tile = []
    for i, seq in enumerate(seqs):
        for hit in aligner.map(seq, cs=True):
            # Convert mappy alignment to substitutions format (0-based to 1-based)
            start = hit.r_st + 1
            end = hit.r_en + 1
            # Find chromosome offset
            chr_offset = calc_chr_offset(chromsizes, hit.ctg)

            substitutions = get_substitutions(hit, seq)
            tv = {
                "id": f"r{i}_{hit.ctg}",
                "from": start + chr_offset,
                "to": end + chr_offset,
                "substitutions": substitutions,
                "color": 0,
            }

            if values:
                tv["extra"] = values[i]

            tile.append(tv)

    def tiles(tile_ids):
        tiles = []

        for tile_id in tile_ids:
            parts = tile_id.split(".")
            z = int(parts[1])
            x = int(parts[2])

            if z != 0 and x != 0:
                tiles += [(tile_id, [])]
            else:
                tiles += [(tile_id, tile)]

        return tiles

    return {"tileset_info": tileset_info, "tiles": tiles}


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


def _chromsizes_from_fasta(fasta_file):
    """Compute chromsizes list from a FASTA file.

    Parameters
    ----------
    fasta_file: str or file-like
        Path to the FASTA file or a binary file-like object.

    Returns
    -------
    list of [str, int]
        List of [sequence_id, length] pairs.
    """
    from Bio import SeqIO
    import io

    if isinstance(fasta_file, str):
        with open(fasta_file, "rb") as fh:
            records = list(SeqIO.parse(io.TextIOWrapper(fh, "utf-8"), "fasta"))
    else:
        content = fasta_file.read()
        fasta_file.seek(0)
        records = list(
            SeqIO.parse(io.TextIOWrapper(io.BytesIO(content), "utf-8"), "fasta")
        )

    return [[r.id, len(r.seq)] for r in records]


def get_local_tiles(
    filename, *csv_args, reffile=None, chromsizes_file=None, **csv_kwargs
):
    """Get local higlass tiles for a pileup-csv file.

    Parameters
    ----------
    filename: str or file-like
        Path to the CSV file or a file-like object.
    reffile: str or file-like, optional
        Path to a FASTA reference file or a file-like object.
        Required when refrow is not provided in csv_kwargs.
    chromsizes_file: str or file-like, optional
        Path to a chromsizes TSV file or a file-like object.
        When omitted, chromsizes are computed from reffile or refrow.
    *csv_args, **csv_kwargs:
        Additional arguments forwarded to csv_sequence_tileset_functions
        (e.g. colname, colnum, header, sep, refrow).
    """
    import pandas as pd
    from clodius.tiles.csv import csv_sequence_tileset_functions

    chromsizes = None
    if chromsizes_file is None:
        if reffile is not None:
            chromsizes = _chromsizes_from_fasta(reffile)
        elif "refrow" in csv_kwargs:
            refrow = csv_kwargs["refrow"]
            sep = csv_kwargs.get("sep", ",")
            header = csv_kwargs.get("header", True)
            colname = csv_kwargs.get("colname")
            colnum = csv_kwargs.get("colnum")
            df = pd.read_csv(filename, header=0 if header else None, sep=sep)
            if colname is None and colnum is not None:
                colname = df.columns[colnum - 1]
            seq = df[colname].values[refrow - 1]
            if not isinstance(seq, str):
                raise TypeError(
                    f"Expected a string sequence in column '{colname}' (colnum={colnum}), "
                    f"but got {type(seq).__name__!r} with value {seq!r}. "
                    f"Available columns are: {list(df.columns)}. "
                    f"Check that colnum/colname points to the sequence column."
                )
            chromsizes = [[f"row_{refrow}", len(seq)]]

    tf = csv_sequence_tileset_functions(
        filename,
        *csv_args,
        tile_functions=tile_functions_fasta if reffile is not None else tile_functions,
        fasta_datafile=reffile,
        chromsizes_datafile=chromsizes_file,
        chromsizes=chromsizes,
        **csv_kwargs,
    )

    tsinfo = tf["tileset_info"]()
    max_resolution = max(tsinfo["resolutions"])

    tile_ids = []

    for i, res in enumerate(sorted(tsinfo["resolutions"], key=lambda x: -x)):
        for j in range(0, max_resolution // res):
            tile_ids += [f"x.{i}.{j}"]

    tiles = dict(tf["tiles"](tile_ids))

    return {"tilesetInfo": {"x": tsinfo}, "tiles": tiles}
