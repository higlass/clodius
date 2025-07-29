import base64
import json
import math
from contextlib import contextmanager
from enum import Enum
from typing import Literal, Union

import h5py
import numpy as np

try:
    import zarr
except ImportError:
    zarr = None

from .utils import abs2genomic


class StorageBackend(Enum):
    """Supported storage backends for multivec datasets."""

    HDF5 = "hdf5"
    if zarr:
        ZARR = "zarr"


def _open_dataset(
    filename: str, storage: Union[StorageBackend, Literal["hdf5", "zarr"]]
) -> Union[h5py.File, zarr.Group]:
    """Internal helper function for opening datasets. Use open_dataset() context manager instead."""
    if isinstance(storage, str):
        storage = StorageBackend(storage)

    if storage == StorageBackend.HDF5:
        return h5py.File(filename, "r")
    elif storage == StorageBackend.ZARR:
        if not zarr:
            raise ImportError("Zarr is not installed")
        return zarr.open_group(filename, mode="r")
    else:
        raise ValueError(f"Unsupported storage backend: {storage}")


def _close_dataset(dataset, storage: Union[StorageBackend, Literal["hdf5", "zarr"]]):
    """Internal helper function for closing datasets. Use open_dataset() context manager instead."""
    if isinstance(storage, str):
        storage = StorageBackend(storage)

    if storage == StorageBackend.HDF5:
        dataset.close()
    elif storage == StorageBackend.ZARR:
        if hasattr(dataset, "store") and hasattr(dataset.store, "close"):
            try:
                dataset.store.close()
            except Exception:
                pass


@contextmanager
def open_dataset(
    filename: str,
    storage: Union[StorageBackend, Literal["hdf5", "zarr"]] = StorageBackend.HDF5,
):
    """
    Context manager for opening and closing multivec datasets.

    Parameters
    ----------
    filename : str
        Path to the dataset file
    storage : StorageBackend or str, optional
        Storage backend to use: StorageBackend.HDF5 or StorageBackend.ZARR

    Yields
    ------
    dataset : h5py.File or zarr.Group
        The opened dataset object
    """
    if isinstance(storage, str):
        storage = StorageBackend(storage)

    dataset = _open_dataset(filename, storage)
    try:
        yield dataset
    finally:
        _close_dataset(dataset, storage)


def tiles(
    filename,
    tile_ids,
    storage: Union[StorageBackend, Literal["hdf5", "zarr"]] = StorageBackend.HDF5,
):
    """
    Retrieve multiple multivec tiles from tids.
    ----------
    filename: string
        The multires file containing the multivec data
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved
    storage: StorageBackend, optional
        Storage backend to use: HDF5 or Zarr (defaults to HDF5)
    """
    f16 = np.finfo("float16")
    f16_min, f16_max = f16.min, f16.max
    generated_tiles = []
    for tile_id in tile_ids:
        tile_pos = [int(i) for i in tile_id.split(".")[1:3]]
        ma = get_single_tile(filename, tile_pos, storage=storage)
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


def get_single_tile(
    filename,
    tile_pos,
    storage: Union[StorageBackend, Literal["hdf5", "zarr"]] = StorageBackend.HDF5,
):
    """
    Retrieve a single multivec tile from a multires file
    Parameters
    ----------
    filename: string
        The multires file containing the multivec data
    tile_pos: (z, x)
        The zoom level and position of this tile
    storage: StorageBackend, optional
        Storage backend to use: HDF5 or Zarr (defaults to HDF5)
    """
    # Handle backward compatibility with string inputs
    if isinstance(storage, str):
        storage = StorageBackend(storage)

    # t1 = time.time()
    tsinfo = tileset_info(filename, storage=storage)

    with open_dataset(filename, storage) as f:
        # print('tileset_info', tileset_info)
        # t2 = time.time()
        # which resolution does this zoom level correspond to?
        resolution = tsinfo["resolutions"][tile_pos[0]]
        tile_size = tsinfo["tile_size"]

        # where in the data does the tile start and end
        tile_start = tile_pos[1] * tile_size * resolution
        tile_end = tile_start + tile_size * resolution

        if storage == StorageBackend.HDF5:
            chromsizes = list(zip(f["chroms"]["name"], f["chroms"]["length"]))
        else:  # zarr
            chrom_names = f["chroms"]["name"][:]
            chrom_lengths = f["chroms"]["length"][:]
            # Handle byte string decoding for zarr
            decoded_names = []
            for name in chrom_names:
                if hasattr(name, "item"):
                    name = name.item()
                if isinstance(name, bytes):
                    decoded_names.append(name.decode("utf-8"))
                else:
                    decoded_names.append(str(name))
            chromsizes = list(zip(decoded_names, chrom_lengths))

        # dense = f['resolutions'][str(resolution)][tile_start:tile_end]
        dense = get_tile(
            f, chromsizes, resolution, tile_start, tile_end, tsinfo["shape"], storage
        )
        # print("dense.shape", dense.shape)

        if len(dense) < tsinfo["tile_size"]:
            # if there aren't enough rows to fill this tile, add some zeros
            dense = np.vstack(
                [
                    dense,
                    np.zeros((tsinfo["tile_size"] - len(dense), tsinfo["shape"][1])),
                ]
            )

    # t3 = time.time()
    # print("single time time: {:.2f} (tileset info: {:.2f}, open time: {:.2f})".format(t3 - t1, t15 - t1, t2 - t15))

    return dense.T


def get_tile(
    f,
    chromsizes,
    resolution,
    start_pos,
    end_pos,
    shape,
    storage: Union[StorageBackend, Literal["hdf5", "zarr"]] = StorageBackend.HDF5,
):
    """
    Get the tile value given the start and end positions and
    chromosome positions.

    Drop bins at the ends of chromosomes if those bins aren't
    full.

    Parameters:
    -----------
    f: h5py.File or zarr.Group
        A file/group containing the data
    chromsizes: [('chr1', 1000), ....]
        An array listing the chromosome sizes
    resolution: int
        The size of each bin, except for the last bin in each
        chromosome.
    start_pos: int
        The start_position of the interval to return
    end_pos: int
        The end position of the interval to return
    storage: StorageBackend
        Storage backend to use: HDF5 or Zarr (defaults to HDF5)

    Returns
    -------
    return_vals: [...]
        A subset of the original genome-wide values containing
        the values for the portion of the genome that is visible.
    """
    # Handle backward compatibility with string inputs
    if isinstance(storage, str):
        storage = StorageBackend(storage)

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
            if storage == StorageBackend.HDF5:
                x = f["resolutions"][str(resolution)]["values"][chrom][
                    start_pos:end_pos
                ]
            else:  # zarr
                x = f["resolutions"][str(resolution)]["values"][chrom][
                    start_pos:end_pos
                ]
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


def tileset_info(
    filename,
    storage: Union[StorageBackend, Literal["hdf5", "zarr"]] = StorageBackend.HDF5,
):
    """
    Return some information about this tileset that will
    help render it in on the client.

    Parameters
    ----------
    filename: str
        The filename of the file containing the tileset info.
    storage: StorageBackend, optional
        Storage backend to use: StorageBackend.HDF5 or StorageBackend.ZARR

    Returns
    -------
    tileset_info: {}
        A dictionary containing the information describing
        this dataset
    """
    # Handle backward compatibility with string inputs
    if isinstance(storage, str):
        storage = StorageBackend(storage)

    # t1 = time.time()
    with open_dataset(filename, storage) as f:
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
        if storage == StorageBackend.HDF5:
            max_pos = [int(sum(f["chroms"]["length"][:]))]
        else:  # zarr
            max_pos = [int(sum(f["chroms"]["length"][:]))]

        # the "rightmost" datapoint position
        # max_pos = [len(f['resolutions']['values'][str(resolutions[-1])])]
        if storage == StorageBackend.HDF5:
            tile_size = int(f["info"].attrs["tile-size"])
            first_chrom = f["chroms"]["name"][0]
        else:  # zarr
            tile_size = int(f["info"].attrs["tile-size"])
            first_chrom_raw = f["chroms"]["name"][0]
            # Handle byte string decoding for zarr
            if hasattr(first_chrom_raw, "item"):
                first_chrom_raw = first_chrom_raw.item()
            if isinstance(first_chrom_raw, bytes):
                first_chrom = first_chrom_raw.decode("utf-8")
            else:
                first_chrom = str(first_chrom_raw)

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

        if storage == StorageBackend.HDF5:
            if "row_infos" in f["resolutions"][str(resolutions[0])].attrs:
                row_infos = f["resolutions"][str(resolutions[0])].attrs["row_infos"]

                if isinstance(row_infos[0], str):
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
                        tileset_info["row_infos"] = [
                            r.decode("utf8") for r in row_infos
                        ]

            elif "row_infos" in f["info"]:
                row_infos_encoded = f["info"]["row_infos"][()]
                tileset_info["row_infos"] = json.loads(row_infos_encoded)
        else:
            if "row_infos" in f["info"].attrs:
                tileset_info["row_infos"] = f["info"].attrs["row_infos"]

    return tileset_info
