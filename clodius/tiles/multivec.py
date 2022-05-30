import base64
import json
import math

import h5py
import numpy as np

from .utils import abs2genomic


def tiles(filename, tile_ids):
    """
    Retrieve multiple multivec tiles from tids.
    ----------
    filename: string
        The multires file containing the multivec data
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved
    """
    f16 = np.finfo("float16")
    f16_min, f16_max = f16.min, f16.max
    generated_tiles = []
    for tile_id in tile_ids:
        tile_pos = [int(i) for i in tile_id.split(".")[1:3]]
        ma = get_single_tile(filename, tile_pos)
        has_nan = np.isnan(ma).any()
        ma_max = ma.max() if ma.size else 0
        ma_min = ma.min() if ma.size else 0
        use_f16 = not has_nan and (ma_min > f16_min and ma_max < f16_max)
        ma = ma.astype(np.float16 if use_f16 else np.float32)
        ma_base64 = base64.b64encode(ma.ravel()).decode("utf-8")
        tile_value = {
            "dense": ma_base64,
            "dtype": "float16" if use_f16 else "float32",
            "shape": ma.shape,
        }
        generated_tiles.append((tile_id, tile_value))

    return generated_tiles


def get_single_tile(filename, tile_pos):
    """
    Retrieve a single multivec tile from a multires file
    Parameters
    ----------
    filename: string
        The multires file containing the multivec data
    tile_pos: (z, x)
        The zoom level and position of this tile
    """
    # t1 = time.time()
    tsinfo = tileset_info(filename)
    f = h5py.File(filename, "r")

    # print('tileset_info', tileset_info)
    # t2 = time.time()
    # which resolution does this zoom level correspond to?
    resolution = tsinfo["resolutions"][tile_pos[0]]
    tile_size = tsinfo["tile_size"]

    # where in the data does the tile start and end
    tile_start = tile_pos[1] * tile_size * resolution
    tile_end = tile_start + tile_size * resolution

    chromsizes = list(zip(f["chroms"]["name"], f["chroms"]["length"]))

    # dense = f['resolutions'][str(resolution)][tile_start:tile_end]
    dense = get_tile(f, chromsizes, resolution, tile_start, tile_end, tsinfo["shape"])
    # print("dense.shape", dense.shape)

    if len(dense) < tsinfo["tile_size"]:
        # if there aren't enough rows to fill this tile, add some zeros
        dense = np.vstack(
            [dense, np.zeros((tsinfo["tile_size"] - len(dense), tsinfo["shape"][1]))]
        )

    f.close()

    # t3 = time.time()
    # print("single time time: {:.2f} (tileset info: {:.2f}, open time: {:.2f})".format(t3 - t1, t15 - t1, t2 - t15))

    return dense.T


def get_tile(f, chromsizes, resolution, start_pos, end_pos, shape):
    """
    Get the tile value given the start and end positions and
    chromosome positions.

    Drop bins at the ends of chromosomes if those bins aren't
    full.

    Parameters:
    -----------
    f: h5py.File
        An hdf5 file containing the data
    chromsizes: [('chr1', 1000), ....]
        An array listing the chromosome sizes
    resolution: int
        The size of each bin, except for the last bin in each
        chromosome.
    start_pos: int
        The start_position of the interval to return
    end_pos: int
        The end position of the interval to return

    Returns
    -------
    return_vals: [...]
        A subset of the original genome-wide values containing
        the values for the portion of the genome that is visible.
    """
    binsize = resolution
    # print('binsize:', binsize)
    # print('start_pos:', start_pos, 'end_pos:', end_pos)
    # print("length:", end_pos - start_pos)
    # print('shape:', shape)

    # t0 = time.time()
    arrays = []
    count = 0

    # keep track of how much data has been returned in bins
    current_binned_data_position = 0
    current_data_position = 0

    num_added = 0
    total_length = 0

    for cid, start, end in abs2genomic([c[1] for c in chromsizes], start_pos, end_pos):
        n_bins = int(np.ceil((end - start) / binsize))
        total_length += end - start
        # print('cid', cid, start, end, 'tl:', total_length)

        try:
            # t1 = time.time()

            chrom = chromsizes[cid][0]

            current_data_position += end - start

            count += 1

            start_pos = math.floor(start / binsize)
            end_pos = math.ceil(end / binsize)

            if start_pos >= end_pos:
                continue

            # print("start:", start, "end", end)
            # print("sp", start_pos * binsize, end_pos * binsize)
            # print('current_data_position:', current_data_position)
            # print('current_binned_data_position:', current_binned_data_position)
            # print('binsize:', binsize, 'resolution:', resolution)

            """
            if start_pos == end_pos:
                if current_data_position - current_binned_data_position > 0:
                    # adding this data as a single bin even though it's not large
                    # enough to cover one bin
                    # print('catching up')
                    end_pos += 1
                else:
                    # print('data smaller than the bin size', start, end, binsize)
                    continue
            """

            # print("offset:", offset, "start_pos", start_pos, end_pos)
            x = f["resolutions"][str(resolution)]["values"][chrom][start_pos:end_pos]
            current_binned_data_position += binsize * (end_pos - start_pos)

            # print("x:", x.shape)

            # If the offset is larger than the binsize, drop the last bin
            offset = current_binned_data_position - current_data_position
            if offset > binsize:
                x = x[:-1]

            # drop the very last bin if it is smaller than the binsize
            """
            if len(x) > 1 and end == clen and clen % binsize != 0:
                # print("dropping")
                x = x[:-1]
            """

            if len(x):
                num_added += len(x)
                # print('cid:', cid, end-start, total_length, 'num_added:', num_added, 'x:', sum(x))

            # t2 = time.time()
            # print("time to fetch {}: {}".format(chrom, t2 - t1))
        except IndexError:
            # beyond the range of the available chromosomes
            # probably means we've requested a range of absolute
            # coordinates that stretch beyond the end of the genome
            # print('zeroes')
            x = np.zeros((n_bins, shape[1]))

        arrays.append(x)

    # print("total_length:", total_length)
    # print('arrays:', len(np.concatenate(arrays)))
    # t3 = time.time()
    # print("total fetch time:", t3 - t0)

    return np.concatenate(arrays)[: shape[0]]


def tileset_info(filename):
    """
    Return some information about this tileset that will
    help render it in on the client.

    Parameters
    ----------
    filename: str
      The filename of the h5py file containing the tileset info.

    Returns
    -------
    tileset_info: {}
      A dictionary containing the information describing
      this dataset
    """
    # t1 = time.time()
    f = h5py.File(filename, "r")
    # t2 = time.time()
    # a sorted list of resolutions, lowest to highest
    # awkward to write because a the numbers representing resolution
    # are datapoints / pixel so lower resolution is actually a higher
    # number
    resolutions = sorted([int(r) for r in f["resolutions"].keys()])[::-1]

    # the "leftmost" datapoint position
    # an array because higlass can display multi-dimensional
    # data
    min_pos = [0]
    max_pos = [int(sum(f["chroms"]["length"][:]))]

    # the "rightmost" datapoint position
    # max_pos = [len(f['resolutions']['values'][str(resolutions[-1])])]
    tile_size = int(f["info"].attrs["tile-size"])
    first_chrom = f["chroms"]["name"][0]

    shape = list(f["resolutions"][str(resolutions[0])]["values"][first_chrom].shape)
    shape[0] = tile_size

    # t3 = time.time()
    # print("tileset info time:", t3 - t2)

    tileset_info = {
        "resolutions": resolutions,
        "min_pos": min_pos,
        "max_pos": max_pos,
        "tile_size": tile_size,
        "shape": shape,
    }

    if "row_infos" in f["resolutions"][str(resolutions[0])].attrs:
        row_infos = f["resolutions"][str(resolutions[0])].attrs["row_infos"]

        if type(row_infos[0]) == str:
            try:
                tileset_info["row_infos"] = [json.loads(r) for r in row_infos]
            except json.JSONDecodeError:
                tileset_info["row_infos"] = [r for r in row_infos]
        else:
            try:
                tileset_info["row_infos"] = [
                    json.loads(r.decode("utf8")) for r in row_infos
                ]
            except json.JSONDecodeError:
                tileset_info["row_infos"] = [r.decode("utf8") for r in row_infos]

    elif "row_infos" in f["info"]:
        row_infos_encoded = f["info"]["row_infos"][()]
        tileset_info["row_infos"] = json.loads(row_infos_encoded)

    f.close()

    return tileset_info
