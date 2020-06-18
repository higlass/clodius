#!/usr/bin/python

import clodius.tiles as ct
import h5py
import math
import numpy as np
import os
import os.path as op
import sys
import time
import argparse


def reduce_data(data_array):
    s = set(data_array)
    lookup_table = dict([(x, i) for i, x in enumerate(s)])

    print("len lookup_table:", len(lookup_table), "len data_array:", len(data_array))
    t1 = time.time()
    new_data = [lookup_table[d] for d in data_array]
    t2 = time.time()

    print("Set size:", len(s), "data size:", len(data_array))
    print("time taken:", 1000000 * (t2 - t1) / len(data_array))
    return (np.array(new_data), list(s))


def main():
    parser = argparse.ArgumentParser(
        description="""

    python main.py
"""
    )

    parser.add_argument("-f", "--filepath", default=None)
    parser.add_argument("-c", "--chunk-size", default=14, type=int)
    parser.add_argument("-z", "--zoom-step", default=8, type=int)
    parser.add_argument("-t", "--tile-size", default=1024, type=int)
    parser.add_argument("-o", "--output-file", default="/tmp/tmp.hdf5")
    # parser.add_argument('-o', '--options', default='yo',
    # help="Some option", type='str')
    # parser.add_argument('-u', '--useless', action='store_true',
    # help='Another useless option')
    args = parser.parse_args()
    last_end = 0
    data = []

    max_zoom = 24
    if op.exists(args.output_file):
        os.remove(args.output_file)
    f = h5py.File(args.output_file, "w")

    hum_size = 3137161264
    tile_size = args.tile_size

    chunk_size = tile_size * 2 ** args.chunk_size

    dsets = []

    # initialize the datasets
    z = 0
    positions = []  # store where we are at the current dataset
    data_buffers = [[]]
    while hum_size / 2 ** z > tile_size:
        dsets += [
            f.create_dataset(
                "values_" + str(z), (hum_size / 2 ** z,), dtype="f", compression="gzip"
            )
        ]
        data_buffers += [[]]
        positions += [0]
        z += args.zoom_step
    d = f.create_dataset("meta", (1,), dtype="f")

    d.attrs["zoom-step"] = args.zoom_step
    d.attrs["max-length"] = hum_size
    d.attrs["assembly"] = "hg19"
    d.attrs["tile-size"] = tile_size
    d.attrs["max-zoom"] = math.ceil(
        math.log(d.attrs["max-length"] / tile_size) / math.log(2)
    )

    print("max_zoom:", d.attrs["max-zoom"])

    if args.filepath is None:
        print("Waiting for input...")
        for line in sys.stdin:
            parts = line.split()
            start = int(parts[0], 10)
            end = int(parts[1], 10)
            # val = float(parts[2])

            if start > last_end:
                # in case there's skipped values in the bed file
                data_buffers[0] += [0] * (last_end - start)

            data_buffers[0] += [float(parts[2])] * (end - start)
            curr_zoom = 0

            while len(data_buffers[curr_zoom]) > chunk_size:
                # get the current chunk and store it
                print("curr_zoom:", curr_zoom)
                curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
                dsets[curr_zoom][
                    positions[curr_zoom] : positions[curr_zoom] + chunk_size
                ] = curr_chunk

                # aggregate and store aggregated values in the next zoom_level's data
                data_buffers[curr_zoom + 1] += list(
                    ct.aggregate(curr_chunk, 2 ** args.zoom_step)
                )
                data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
                positions[curr_zoom] += chunk_size
                data = data_buffers[curr_zoom + 1]
                curr_zoom += 1

        # store the remaining data
        print("tile_size:", tile_size, positions[0])

        while True:
            # get the current chunk and store it
            chunk_size = len(data_buffers[curr_zoom])
            curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
            dsets[curr_zoom][
                positions[curr_zoom] : positions[curr_zoom] + chunk_size
            ] = curr_chunk

            print(
                "curr_zoom:",
                curr_zoom,
                "position:",
                positions[curr_zoom] + len(curr_chunk),
            )
            print("len:", [len(d) for d in data_buffers])

            # aggregate and store aggregated values in the next zoom_level's data
            data_buffers[curr_zoom + 1] += list(
                ct.aggregate(curr_chunk, 2 ** args.zoom_step)
            )
            data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
            positions[curr_zoom] += chunk_size
            data = data_buffers[curr_zoom + 1]
            curr_zoom += 1

            # we've created enough tile levels to cover the entire maximum width
            if curr_zoom * args.zoom_step >= max_zoom:
                break

    # still need to take care of the last chunk

    data = np.array(data)

    """
    curr_zoom = 0
    dsets = []
    while len(data) > tile_size:
        (to_store_data, values_list) = reduce_data(data)
        if len(values_list) < 2 **16 and len(data) > 2**20:
            print "storing lookup"
            dsets += [f.create_dataset("zoom_" + str(curr_zoom), (len(data),), dtype='i2', compression='gzip')]
            lookup_dset = f.create_dataset("values_" + str(curr_zoom), (len(values_list), ), dtype='f')
            lookup_dset[:] = np.array(values_list)

            dsets[curr_zoom][:len(data)] = to_store_data
        else:
            print "storing raw...."
            dsets += [f.create_dataset(str(curr_zoom), (len(data),), dtype='f4', compression='gzip')]
            dsets[curr_zoom][:len(data)] = data

        new_data = ct.aggregate(data, 32)
        data = new_data
        curr_zoom += 1

    dsets += [f.create_dataset(str(curr_zoom), (len(data),), dtype='i2', compression='gzip')]
    dsets[curr_zoom][:len(data)] = data

    print "time:", time.time() - t1
    print "len(data):", len(data)
    """

    # print data


if __name__ == "__main__":
    main()
