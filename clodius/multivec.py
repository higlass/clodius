from __future__ import print_function

import h5py
import math
import numpy as np
import os
import os.path as op
import sys

def create_multivec_multires(array_data, chromsizes, 
                    agg, starting_resolution=1,
                    tile_size=1024, output_file='/tmp/my_file.multires',
                    row_infos=None):
    '''
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
    '''
    filename = output_file

    # this is just so we can run this code
    # multiple times without h5py complaining
    if op.exists(filename):
        os.remove(filename)

    # this will be the file that contains our multires data
    f = h5py.File(filename, 'w')
        
    # store some metadata
    f.create_group('info')
    f['info'].attrs['tile-size'] = tile_size
    
    f.create_group('resolutions')
    f.create_group('chroms')

    # start with a resolution of 1 element per pixel
    curr_resolution = starting_resolution

    # this will be our sample highest-resolution array
    # and it will be stored under the resolutions['1']
    # dataset
    f['resolutions'].create_group(str(curr_resolution))

    chroms, lengths = zip(*chromsizes)
    chrom_array = np.array(chroms, dtype='S')

    row_infos = None
    print('array_data.attrs', list(array_data.attrs.keys()))
    if 'row_infos' in array_data.attrs:
        row_infos = array_data.attrs['row_infos']
        print("adding row_infos")

    # add the chromosome information
    if row_infos is not None:
        print("adding row_infos1")
        f['resolutions'][str(curr_resolution)].attrs.create('row_infos', row_infos)

    f['resolutions'][str(curr_resolution)].create_group('chroms')
    f['resolutions'][str(curr_resolution)].create_group('values')
    f['resolutions'][str(curr_resolution)]['chroms'].create_dataset('name', shape=(len(chroms),), dtype=chrom_array.dtype, data=chrom_array, compression='gzip')
    f['resolutions'][str(curr_resolution)]['chroms'].create_dataset('length', shape=(len(chroms),), data=lengths, compression='gzip')

    f['chroms'].create_dataset('name', shape=(len(chroms),), dtype=chrom_array.dtype, data=chrom_array, compression='gzip')
    f['chroms'].create_dataset('length', shape=(len(chroms),), data=lengths, compression='gzip')

    # add the data
    for chrom,length in zip(chroms, lengths):
        if not chrom in array_data:
            print("Missing chrom {} in input file".format(chrom), file=sys.stderr)
            continue

        f['resolutions'][str(curr_resolution)]['values'].create_dataset(str(chrom), array_data[chrom].shape, compression='gzip')
        print("array_data.shape", array_data[chrom].shape)
        f['resolutions'][str(curr_resolution)]['values'][chrom][:] = array_data[chrom]    # see above section
        

    # the maximum zoom level corresponds to the number of aggregations
    # that need to be performed so that the entire extent of
    # the dataset fits into one tile
    total_length = sum(lengths)
    print("total_length:", total_length, "tile_size:", tile_size, "starting_resolution:", starting_resolution)
    max_zoom = math.ceil(math.log(total_length / (tile_size * starting_resolution) ) / math.log(2))
    print("max_zoom:", max_zoom)
    
    # we're going to go through and create the data for the different
    # zoom levels by summing adjacent data points
    prev_resolution = curr_resolution

    for i in range(max_zoom):
        # each subsequent zoom level will have half as much data
        # as the previous
        curr_resolution = prev_resolution * 2
        f['resolutions'].create_group(str(curr_resolution))

        # add information about each of the rows
        if row_infos is not None:
            print("adding row_infos2")
            f['resolutions'][str(curr_resolution)].attrs.create('row_infos', row_infos)

        f['resolutions'][str(curr_resolution)].create_group('chroms')
        f['resolutions'][str(curr_resolution)].create_group('values')
        f['resolutions'][str(curr_resolution)]['chroms'].create_dataset('name', shape=(len(chroms),), dtype=chrom_array.dtype, data=chrom_array, compression='gzip')
        f['resolutions'][str(curr_resolution)]['chroms'].create_dataset('length', shape=(len(chroms),), data=lengths, compression='gzip')

        for chrom,length in zip(chroms, lengths):
            if chrom not in f['resolutions'][str(prev_resolution)]['values']:
                continue

            next_level_length = math.ceil(
                len(f['resolutions'][str(prev_resolution)]['values'][chrom]) / 2)

            old_data = f['resolutions'][str(prev_resolution)]['values'][chrom][:]
            #print("prev_resolution:", prev_resolution)
            #print("old_data.shape", old_data.shape)

            # this is a sort of roundabout way of calculating the 
            # shape of the aggregated array, but all its doing is
            # just halving the first dimension of the previous shape
            # without taking into account the other dimensions
            new_shape = list(old_data.shape)
            new_shape[0] = math.ceil(new_shape[0] / 2)
            new_shape = tuple(new_shape)

            f['resolutions'][str(curr_resolution)]['values'].create_dataset(chrom, 
                                            new_shape, compression='gzip')

            #print("11 old_data.shape", old_data.shape)
            if len(old_data) % 2 != 0:
                # we need our array to have an even number of elements
                # so we just add the last element again
                old_data = np.concatenate((old_data, [old_data[-1]]))
            #print("22 old_data.shape", old_data.shape)

            #print('old_data:', old_data)
            #print("shape:", old_data.shape)
            # actually sum the adjacent elements
            #print("old_data.shape", old_data.shape)
            new_data = agg(old_data)

            '''
            print("zoom_level:", max_zoom - 1 - i, 
                  "resolution:", curr_resolution, 
                  "new_data length", len(new_data))
            '''
            f['resolutions'][str(curr_resolution)]['values'][chrom][:] = new_data

        prev_resolution = curr_resolution
    return f
