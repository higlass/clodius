from __future__ import print_function

import gzip
import h5py
import math
import numpy as np
import logging
import os
import os.path as op
import sys

logger = logging.getLogger(__name__)


def bedfile_to_multivec(
    input_filenames,
    f_out,
    bedline_to_chrom_start_end_vector,
    base_resolution,
    has_header,
    chunk_size,
    num_rows,
    row_infos=None,
):
    """
    Convert an epilogos bedfile to multivec format.
    """

    files = []
    for input_filename in input_filenames:
        if op.splitext(input_filename)[1] == ".gz":
            files += [gzip.open(input_filename, "rt")]
        else:
            files += [open(input_filename, "r")]

    FILL_VALUE = np.nan

    # batch regions because h5py is really bad at writing
    batch_length = chunk_size
    batch = []

    # the current index in the dataset
    curr_index = 0
    # the start of the batch in the dataset
    batch_start_index = 0

    if has_header:
        files[0].readline()

    prev_chrom = None

    print("base_resolution:", base_resolution)
    for _, lines in enumerate(zip(*files)):

        # Identifies bedfile headers and ignore them
        if lines[0].startswith("browser") or lines[0].startswith("track"):
            continue

        chrom, start, end, vector = bedline_to_chrom_start_end_vector(lines, row_infos)
        # if vector[0] > 0 or vector[1] > 0:
        if len(vector) < len(lines) * num_rows:
            logger.warning("Lines contain fewer columns than expected: %s", lines)
            vector += [np.nan] * (len(lines) * num_rows - len(vector))

        if start % base_resolution != 0:
            logger.error(
                "The start coordinate is not a multiple of the resolution in line: %s",
                lines,
            )
            sys.exit(1)

        if prev_chrom is not None and chrom != prev_chrom:
            # we've reached a new chromosome so we'll dump all
            # the previous values
            # print("len(batch:", len(batch),
            #       "batch_start_index", batch_start_index)
            f_out[prev_chrom][
                batch_start_index : batch_start_index + len(batch)
            ] = np.array(batch)

            # we're starting a new chromosome so we start from the beginning
            curr_index = 0
            batch_start_index = 0
            batch = []

        prev_chrom = chrom

        # print('parts', parts)
        # print('chrom:', chrom, start)

        data_start_index = start // base_resolution

        # if the bedfile skips over a region, we have to add it as empty values
        # to preserve our batch writing
        while curr_index < data_start_index:
            batch += [[FILL_VALUE] * len(vector)]
            curr_index += 1

        """
        if curr_index != data_start_index:
            print("curr_index:", curr_index, data_start_index)
            print("line:", line)
        """

        if curr_index != data_start_index:
            message = """
The expected position location does not match the observed location at entry {}:{}-{}
This is probably because the bedfile is not sorted. Please sort and try again.
            """.format(chrom, start, end)
            raise ValueError(message)
        # assert curr_index == data_start_index, message
        # print('vector', vector)

        # When the binsize is not equal to the base_resolution
        # "break down" the binsize into bins of the rbase_esolution size
        # and add the values to each bin.
        # If there is a remainder, add an additional bin
        data_end_index = math.ceil(end / base_resolution)

        while curr_index < data_end_index:
            batch += [vector]
            curr_index += 1

        # fill in empty

        if len(batch) >= batch_length:
            # dump batch
            try:
                f_out[chrom][
                    batch_start_index : batch_start_index + len(batch)
                ] = np.array(batch)
            except TypeError as ex:
                print("Error:", ex, file=sys.stderr)
                print("Probably need to set the --num-rows parameter", file=sys.stderr)
                return

            batch = []
            batch_start_index = curr_index
            print("dumping batch:", chrom, batch_start_index)

    f_out[chrom][batch_start_index : batch_start_index + len(batch)] = np.array(batch)


def create_multivec_multires(
    array_data,
    chromsizes,
    agg,
    starting_resolution=1,
    tile_size=1024,
    output_file="/tmp/my_file.multires",
    row_infos=None,
):
    """
    Create a multires file containing the array data
    aggregated at multiple resolutions.

    Parameters
    ----------
    array_data: {'chrom_key': np.array, }
        The array data to aggregate organized by chromosome
    chromsizes: [('chrom_key', size),...]
    agg: lambda
        The function that will aggregate the data. Should
        take an array as input and create another array of
        roughly half the length
    starting_resolution: int (default 1)
        The starting resolution of the input data
    tile_size: int
        The tile size that we want higlass to use. This should
        depend on the size of the data but when in doubt, just use
        256.
    """
    filename = output_file

    # this is just so we can run this code
    # multiple times without h5py complaining
    if op.exists(filename):
        os.remove(filename)

    # this will be the file that contains our multires data
    f = h5py.File(filename, "w")

    # store some metadata
    f.create_group("info")
    f["info"].attrs["tile-size"] = tile_size

    f.create_group("resolutions")
    f.create_group("chroms")

    # start with a resolution of 1 element per pixel
    curr_resolution = starting_resolution

    # this will be our sample highest-resolution array
    # and it will be stored under the resolutions['1']
    # dataset
    f["resolutions"].create_group(str(curr_resolution))

    chroms, lengths = zip(*chromsizes)
    chrom_array = np.array(chroms, dtype="S")

    # row_infos = None
    if "row_infos" in array_data.attrs:
        row_infos = array_data.attrs["row_infos"]

    # add the chromosome information
    if row_infos is not None:
        f["resolutions"][str(curr_resolution)].attrs.create("row_infos", row_infos)

    f["resolutions"][str(curr_resolution)].create_group("chroms")
    f["resolutions"][str(curr_resolution)].create_group("values")
    f["resolutions"][str(curr_resolution)]["chroms"].create_dataset(
        "name",
        shape=(len(chroms),),
        dtype=chrom_array.dtype,
        data=chrom_array,
        compression="gzip",
    )
    f["resolutions"][str(curr_resolution)]["chroms"].create_dataset(
        "length", shape=(len(chroms),), data=lengths, compression="gzip"
    )

    f["chroms"].create_dataset(
        "name",
        shape=(len(chroms),),
        dtype=chrom_array.dtype,
        data=chrom_array,
        compression="gzip",
    )
    f["chroms"].create_dataset(
        "length", shape=(len(chroms),), data=lengths, compression="gzip"
    )

    # add the data
    for chrom, length in zip(chroms, lengths):
        if chrom not in array_data:
            print("Missing chrom {} in input file".format(chrom), file=sys.stderr)
            continue

        # print("creating new dataset")
        f["resolutions"][str(curr_resolution)]["values"].create_dataset(
            str(chrom), array_data[chrom].shape, compression="gzip"
        )
        standard_chunk_size = 1e5
        start = 0
        chrom_data = f["resolutions"][str(curr_resolution)]["values"][chrom]

        chunk_size = int(min(standard_chunk_size, len(chrom_data)))
        # print("array_data.shape", array_data[chrom].shape)

        while start < len(chrom_data):
            # see above section
            chrom_data[start : start + chunk_size] = array_data[chrom][
                start : start + chunk_size
            ]
            start += int(min(standard_chunk_size, len(array_data[chrom]) - start))

    # the maximum zoom level corresponds to the number of aggregations
    # that need to be performed so that the entire extent of
    # the dataset fits into one tile
    total_length = sum(lengths)
    # print("total_length:", total_length, "tile_size:", tile_size, "starting_resolution:", starting_resolution)
    max_zoom = math.ceil(
        math.log(total_length / (tile_size * starting_resolution)) / math.log(2)
    )

    # we're going to go through and create the data for the different
    # zoom levels by summing adjacent data points
    prev_resolution = curr_resolution

    for i in range(max_zoom):
        # each subsequent zoom level will have half as much data
        # as the previous
        curr_resolution = prev_resolution * 2
        f["resolutions"].create_group(str(curr_resolution))

        # add information about each of the rows
        if row_infos is not None:
            f["resolutions"][str(curr_resolution)].attrs.create("row_infos", row_infos)

        f["resolutions"][str(curr_resolution)].create_group("chroms")
        f["resolutions"][str(curr_resolution)].create_group("values")
        f["resolutions"][str(curr_resolution)]["chroms"].create_dataset(
            "name",
            shape=(len(chroms),),
            dtype=chrom_array.dtype,
            data=chrom_array,
            compression="gzip",
        )
        f["resolutions"][str(curr_resolution)]["chroms"].create_dataset(
            "length", shape=(len(chroms),), data=lengths, compression="gzip"
        )

        for chrom, length in zip(chroms, lengths):
            if chrom not in f["resolutions"][str(prev_resolution)]["values"]:
                continue

            # next_level_length = math.ceil(
            #     len(f['resolutions'][str(prev_resolution)]['values'][chrom]) / 2)

            start = 0

            chrom_data = f["resolutions"][str(prev_resolution)]["values"][chrom]

            standard_chunk_size = 1e5
            chunk_size = int(min(standard_chunk_size, len(chrom_data)))

            new_shape = list(chrom_data.shape)
            new_shape[0] = math.ceil(new_shape[0] / 2)
            new_shape = tuple(new_shape)

            f["resolutions"][str(curr_resolution)]["values"].create_dataset(
                chrom, new_shape, compression="gzip"
            )

            while start < len(chrom_data):
                old_data = f["resolutions"][str(prev_resolution)]["values"][chrom][
                    start : start + chunk_size
                ]
                # print("prev_resolution:", prev_resolution)
                # print("old_data.shape", old_data.shape)

                # this is a sort of roundabout way of calculating the
                # shape of the aggregated array, but all its doing is
                # just halving the first dimension of the previous shape
                # without taking into account the other dimensions
                # print("11 old_data.shape", old_data.shape)
                if len(old_data) % 2 != 0:
                    # we need our array to have an even number of elements
                    # so we just add the last element again
                    old_data = np.concatenate((old_data, [old_data[-1]]))
                    chunk_size += 1
                # print("22 old_data.shape", old_data.shape)

                # print('old_data:', old_data)
                # print("shape:", old_data.shape)
                # actually sum the adjacent elements
                # print("old_data.shape", old_data.shape)
                new_data = agg(old_data)

                """
                print("zoom_level:", max_zoom - 1 - i,
                      "resolution:", curr_resolution,
                      "new_data length", len(new_data))
                """
                f["resolutions"][str(curr_resolution)]["values"][chrom][
                    int(start / 2) : int(start / 2 + chunk_size / 2)
                ] = new_data
                start += int(min(standard_chunk_size, len(chrom_data) - start))

        prev_resolution = curr_resolution
    return f
