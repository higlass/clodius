import ast
import logging
import math
import os
import os.path as op
import tempfile
from tempfile import TemporaryDirectory

import h5py
import numpy as np

import click
import clodius.chromosomes as cch
import clodius.multivec as cmv
import negspy.coordinates as nc
import scipy.misc as sm
from clodius.tiles.bam import get_cigar_substitutions

from . import cli

logger = logging.getLogger(__name__)


def epilogos_bedline_to_vector(bedlines, row_infos=None):
    """
    Convert a line from an epilogos bedfile to vector format.

    Parameters
    -----------
    bedline: [string,....]
        A line from a bedfile broken up into its constituent parts
        (e.g. ["chr1", "1000", "2000", "[1,2,34,5]"])

    Returns
    -------
    An array containing the values associated with that line
    """
    bedline = bedlines[0]
    parts = bedline.strip().split("\t")
    # extract the state values e.g. [...,[0,14],[0.56,15]]
    array_str = parts[3].split(":")[-1]

    # make sure they're ordered by their index
    array_val = sorted(ast.literal_eval(array_str), key=lambda x: x[1])
    states = [v[0] for v in array_val]

    chrom = parts[0]
    start = int(parts[1])
    end = int(parts[2])

    return (chrom, start, end, states)


def states_bedline_to_vector(bedlines, states_dic):
    """
    Convert a line from a bedfile containing states in categorical data to vector format.

    Parameters
    ----------

    bedline: [string,...]
        A line form a bedfile broken up into its contituent parts
        (e.g. ["chr1", "1000", "2000", "state"]))


    states_dic: {'key':val,...}
        A dictionary containing the states in the file with a corresponding value
        (e.g. {'state1_name': 1, 'state2_name': 2,...})

    Returns
    -------

    Four variables containing the values associated with that line: chrom, start, end, states_vector
    (e.g. chrom = "chr1", start = 1000, end = 2000, states_vector = [1,0,0,0])
    """
    # we support passing in multiple bed files for multivec creation from
    # other file types, but this one only supports a single file so just
    # assume that a single file is passed in
    bedline = bedlines[0]
    parts = bedline.strip().split("\t")
    chrom = parts[0]
    start = int(parts[1])
    end = int(parts[2])
    state = states_dic[parts[3]]

    states_vector = [1 if index == state else 0 for index in range(len(states_dic))]

    return (chrom, start, end, states_vector)


@cli.group()
def convert():
    """
    Aggregate a data file so that it stores the data at multiple
    resolutions.
    """
    pass


def _bedgraph_to_multivec(
    filepaths,
    output_file,
    assembly,
    chrom_col,
    from_pos_col,
    to_pos_col,
    value_col,
    has_header,
    chunk_size,
    nan_value,
    chromsizes_filename,
    starting_resolution,
    num_rows,
    format,
    row_infos_filename,
    tile_size,
    method,
):
    print("chrom_col:", chrom_col)

    with tempfile.TemporaryDirectory() as td:
        print("temporary dir:", td)

        temp_file = op.join(td, "temp.mv5")
        f_out = h5py.File(temp_file, "w")

        (chrom_info, chrom_names, chrom_sizes) = cch.load_chromsizes(
            chromsizes_filename, assembly
        )

        if row_infos_filename is not None:
            with open(row_infos_filename, "r") as f:
                row_infos = [line.strip().encode("utf8") for line in f]

        else:
            row_infos = None

        for chrom in chrom_info.chrom_order:
            f_out.create_dataset(
                chrom,
                (
                    math.ceil(chrom_info.chrom_lengths[chrom] / starting_resolution),
                    num_rows * len(filepaths),
                ),
                fillvalue=np.nan,
                compression="gzip",
            )

        def bedline_to_chrom_start_end_vector(bedlines, row_infos=None):
            chrom_set = set()
            start_set = set()
            end_set = set()
            all_vector = []

            for bedline in bedlines:
                parts = bedline.strip().split()
                chrom = parts[chrom_col - 1]
                start = int(parts[from_pos_col - 1])
                end = int(parts[to_pos_col - 1])
                vector = [
                    float(f) if not f == "NA" else np.nan
                    for f in parts[value_col - 1 : value_col - 1 + num_rows]
                ]
                chrom_set.add(chrom)
                start_set.add(start)
                end_set.add(end)

                if len(chrom_set) > 1:
                    raise ValueError(
                        "Chromosomes don't match in these lines:", bedlines
                    )
                if len(start_set) > 1:
                    raise ValueError(
                        "Start positions don't match in these lines:", bedlines
                    )
                if len(end_set) > 1:
                    raise ValueError(
                        "End positions don't match in these lines:", bedlines
                    )
                all_vector += vector

            return (
                list(chrom_set)[0],
                list(start_set)[0],
                list(end_set)[0],
                all_vector,
            )

        if format == "epilogos":
            cmv.bedfile_to_multivec(
                filepaths,
                f_out,
                epilogos_bedline_to_vector,
                starting_resolution,
                has_header,
                chunk_size,
                num_rows,
            )
        elif format == "states":
            assert (
                row_infos is not None
            ), "A row_infos file must be provided for --format = 'states' "
            states_names = [lne.decode("utf8").split("\t")[0] for lne in row_infos]
            states_dic = {states_names[x]: x for x in range(len(row_infos))}

            cmv.bedfile_to_multivec(
                filepaths,
                f_out,
                states_bedline_to_vector,
                starting_resolution,
                has_header,
                chunk_size,
                num_rows,
                states_dic,
            )
        else:
            cmv.bedfile_to_multivec(
                filepaths,
                f_out,
                bedline_to_chrom_start_end_vector,
                starting_resolution,
                has_header,
                chunk_size,
                num_rows,
            )

        f_out.close()
        tf = temp_file
        f_in = h5py.File(tf, "r")

        if output_file is None:
            output_file = op.splitext(filepaths[0])[0] + ".multires.mv5"
        print("output_file:", output_file)

        # Override the output file if it existts
        if op.exists(output_file):
            os.remove(output_file)

        if method == "logsumexp":

            def agg(x):
                # newshape = (x.shape[2], -1, 2)
                # b = x.T.reshape((-1,))

                a = x.T.reshape((x.shape[1], -1, 2))

                # this is going to be an odd way to get rid of nan
                # values
                orig_shape = a.shape
                na = a.reshape((-1,))

                SMALL_NUM = -1e8
                NAN_THRESHOLD_NUM = SMALL_NUM / 100

                if np.nanmin(na) < NAN_THRESHOLD_NUM:
                    raise ValueError(
                        "Error removing nan's when running logsumexp aggregation"
                    )

                na[np.isnan(na)] = SMALL_NUM
                na = na.reshape(orig_shape)
                res = sm.logsumexp(a, axis=2).T

                nres = res.reshape((-1,))
                # print("nres:", np.nansum(nres < NAN_THRESHOLD_NUM))
                nres[nres < NAN_THRESHOLD_NUM] = np.nan
                res = nres.reshape(res.shape)

                # print("res:", np.nansum(res.reshape((-1,))))

                return res

        else:

            def agg(x):
                return x.T.reshape((x.shape[1], -1, 2)).sum(axis=2).T

        if format == "states":
            states_row_infos = [
                state_name.encode("utf8") for state_name in states_names
            ]
            cmv.create_multivec_multires(
                f_in,
                chromsizes=zip(chrom_names, chrom_sizes),
                agg=agg,
                starting_resolution=starting_resolution,
                tile_size=tile_size,
                output_file=output_file,
                row_infos=states_row_infos,
            )
        else:
            cmv.create_multivec_multires(
                f_in,
                chromsizes=zip(chrom_names, chrom_sizes),
                agg=agg,
                starting_resolution=starting_resolution,
                tile_size=tile_size,
                output_file=output_file,
                row_infos=row_infos,
            )


@convert.command()
@click.argument("filepaths", metavar="FILEPATHS", nargs=-1)
@click.option(
    "--output-file",
    "-o",
    default=None,
    help="The default output file name to use. If this isn't"
    "specified, clodius will replace the current extension"
    "with .hitile",
)
@click.option(
    "--assembly",
    "-a",
    help="The genome assembly that this file was created against",
    type=click.Choice(nc.available_chromsizes()),
    default="hg19",
)
@click.option(
    "--chromosome-col",
    help="The column number (1-based) which contains the chromosome " "name",
    default=1,
    type=int,
)
@click.option(
    "--from-pos-col",
    help="The column number (1-based) which contains the starting " "position",
    default=2,
    type=int,
)
@click.option(
    "--to-pos-col",
    help="The column number (1-based) which contains the ending" "position",
    default=3,
    type=int,
)
@click.option(
    "--value-col",
    help="The column number (1-based) which contains the actual value" "position",
    default=4,
    type=int,
)
@click.option(
    "--has-header/--no-header",
    help="Does this file have a header that we should ignore",
    default=False,
)
@click.option(
    "--chunk-size", help="The size of the chunks to read in at once", default=1e5
)
@click.option(
    "--nan-value", help="The string to use as a NaN value", type=str, default=None
)
@click.option(
    "--chromsizes-filename",
    help="A file containing chromosome sizes and order",
    default=None,
)
@click.option(
    "--starting-resolution",
    help="The base resolution of the data. Used to determine how much space to allocate"
    " in the multivec file",
    default=1,
)
@click.option(
    "--num-rows",
    help="The number of rows at each position in the multivec format",
    default=1,
)
@click.option(
    "--format",
    type=click.Choice(["default", "epilogos", "states"]),
    help="'default':chr start end state1_value state2_value, etc;"
    "'epilogos': chr start end [[state1_value, state1_num],[state2_value, state2_num],[etc]];"
    "'states': chr start end state_name",
    default="default",
)
@click.option(
    "--row-infos-filename",
    help="A file containing the names of the rows in the multivec file",
    default=None,
)
@click.option(
    "--tile-size",
    "-t",
    default=256,
    help="The number of data points in each tile."
    "Used to determine the number of zoom levels"
    "to create.",
)
@click.option(
    "--method",
    help="The method used to aggregate values (e.g. sum, average...)",
    type=click.Choice(["sum", "logsumexp"]),
    default="sum",
)
def bedfile_to_multivec(
    filepaths,
    output_file,
    assembly,
    chromosome_col,
    from_pos_col,
    to_pos_col,
    value_col,
    has_header,
    chunk_size,
    nan_value,
    chromsizes_filename,
    starting_resolution,
    num_rows,
    format,
    row_infos_filename,
    tile_size,
    method,
):
    _bedgraph_to_multivec(
        filepaths,
        output_file,
        assembly,
        chromosome_col,
        from_pos_col,
        to_pos_col,
        value_col,
        has_header,
        chunk_size,
        nan_value,
        chromsizes_filename,
        starting_resolution,
        num_rows,
        format,
        row_infos_filename,
        tile_size,
        method,
    )


def reads_to_array(f_in, h_out, ref, chrom_len):
    """Convert BAM file reads to an HDF5 array.

    Arguments:

    f_in: The pysam AlignmentFile handle
    h_out: An hdf5 file handle to store the output arrays
    ref: The chromosome name
    chrom_len: The length of the chromosome

    """
    logger.info("Creating array for chrom: %s with length: %d", ref, chrom_len)
    reads = f_in.fetch(ref, 0, chrom_len)

    arr = np.zeros((10, chrom_len))

    subs = {
        "A": 0,
        "C": 1,
        "G": 2,
        "T": 3,
        "S": 4,
        "M": 5,
        "I": 6,
        "D": 7,
        "H": 8,
        "N": 9
    }

    logger.info("Finished allocating arrays")
    counter = 0

    for read in reads:
        counter += 1

        if counter % 5000 == 0:
            logger.info("Processed %d reads", counter)

        try:
            ap = [
                p
                for p in read.get_aligned_pairs(with_seq=True, matches_only=True)
                if p[2].islower()
            ]
        except TypeError as te:
            logger.error("Type error: %s", str(te))
            logger.error("Read: %s", str(read))
            continue
        #     print("read", read.reference_start)
        arr[subs["M"]][read.reference_start + 1 : read.reference_end + 1] += 1

        for start, op, oplen in get_cigar_substitutions(read):
            if op == "I":
                arr[subs[op]][start + 1] += 1
            else:
                arr[subs[op]][start + 1 : start + 1 + oplen] += 1

        for p in ap:
            arr[subs["M"]][p[1] + 1] -= 1
            arr[subs[read.query_sequence[p[0]]]][p[1] + 1] += 1

    logger.info("Dumping array with shape: %s", str(arr.T.shape))

    h_out.create_dataset(ref, data=arr.T, compression="gzip")
    pass


def sum_agg(x):
    return np.nansum(x.T.reshape((x.shape[1], -1, 2)), axis=2).T


@convert.command()
@click.argument("filepath")
@click.option("--index-filepath", "-i", default=None)
@click.option(
    "--output-file",
    "-o",
    default=None,
    help="The default output file name to use. If this isn't"
    "specified, clodius will replace the current extension"
    "with .bam.mv5",
)
def bamfile_to_multivec(filepath, index_filepath, output_file):
    """Convert a BAM file to a multivec representation."""
    import pysam

    logging.basicConfig(level=logging.INFO)

    if index_filepath is None:
        index_filepath = filepath + ".bai"

    if output_file is None:
        output_file = op.splitext(filepath)[0] + ".bam.mv5"
    logger.info("Output file: %s", output_file)

    import numpy as np
    from clodius.tiles.bam import get_cigar_substitutions

    f = pysam.AlignmentFile(filepath, index_filename=index_filepath)

    logger.info("Loaded alignment file")

    with TemporaryDirectory() as tmp_dir:
        h_mid = h5py.File(op.join(tmp_dir, "mid.h5"), "w")

        for ref, chrom_len in zip(f.references, f.lengths):
            reads_to_array(f, h_mid, ref, chrom_len)

        logger.info("Creating multivec array")
        cmv.create_multivec_multires(
            h_mid,
            zip(f.references, f.lengths),
            agg=sum_agg,
            #     agg=log_sum_exp_agg,
            starting_resolution=1,
            row_infos=["a", "t", "g", "c", "s", "m", "i", "d", "h", "n"],
            output_file=output_file,
            tile_size=256,
        )
