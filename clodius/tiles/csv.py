from clodius.chromosomes import chromsizes_as_array
import io


# @lru_cache
def csv_sequence_tileset_functions(
    filename,
    tile_functions,
    colname=None,
    colnum=None,
    header=True,
    sep=",",
    refrow=None,
    fasta_datafile=None,
    chromsizes_datafile=None,
    chromsizes=None,
):
    """Read a csv file and return a list of sequences.

    Parameters
    ----------
    filename: string
        The name of the csv file
    tile_functions:
        A function that will take a list of sequences as a parameters
        and return tileset_info and tiles functions
    colname: Optional[str]
        The name of the column containing the sequences.
    colnum: Optional[int]
        The column number of the sequence logo file. 0-based.
        Only used if colname is not provided.
    sep: string
        The separator used in the csv file
    refrow: A row to use as a reference sequence when calculating
        alignments. Should be 1-based
    fasta_datafile: A fasta file to align the sequences to.
    """
    import pandas as pd

    if not header:
        header = None
    else:
        header = 0

    if not colname and not colnum:
        raise ValueError("No colname or colnum specified")

    df = pd.read_csv(filename, header=header, sep=sep)

    if not colname:
        colname = df.columns[colnum - 1]

    sequences = df[colname].values

    if refrow:
        refseqs = [{"id": f"row_{refrow}", "seq": sequences[refrow - 1]}]
        if chromsizes is None:
            chromsizes = [[f"row_{refrow}", len(sequences[refrow - 1])]]
    else:
        if fasta_datafile:
            from Bio import SeqIO

            if isinstance(fasta_datafile, str):
                fasta_handle = open(fasta_datafile, "rb")
            else:
                fasta_handle = fasta_datafile

            refseqs = [
                {"id": record.id, "seq": str(record.seq)}
                for record in SeqIO.parse(
                    io.TextIOWrapper(fasta_handle, "utf-8"), "fasta"
                )
            ]

            if chromsizes is None:
                if chromsizes_datafile:
                    chromsizes = chromsizes_as_array(chromsizes_datafile)
                else:
                    chromsizes = [[r["id"], len(r["seq"])] for r in refseqs]
        else:
            raise ValueError("No reference row or fasta file provided")

    tf = tile_functions(
        sequences,
        refseqs=refseqs,
        values=df.to_dict(orient="records"),
        chromsizes=chromsizes,
    )

    orig_tsinfo = tf["tileset_info"]()
    # Decorate the tileset info function so that it returns
    # the column names as well.
    tf["tileset_info"] = lambda: {"columns": list(df.columns), **orig_tsinfo}

    return tf
