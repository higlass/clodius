import base64
import h5py
import math
import numpy as np
import os
import os.path as op
import sys

def array_to_hitile(old_data, filename, zoom_step=8, chunks=(1e6,), agg_function=np.sum):
    '''
    Downsample a dataset so that it's compatible with HiGlass (filetype: hitile, datatype: vector)
    
    Parameters
    ----------
    old_data: np.array
        A numpy array containing the data to be downsampled
    filename: string
        The output filename where the resulting multi-resolution
        data will be stored.
    zoom_step: int
        The number of zoom levels to skip when aggregating
    '''
    import dask.array as da

    if op.exists(filename):
        os.remove(filename)
    
    f_new = h5py.File(filename, 'w')
    
    tile_size = 1024
    min_pos = 0
    max_pos = len(old_data)


    # we store every n'th zoom level
    zoom_factor = 2 ** zoom_step
    
    max_zoom = math.ceil(math.log(max_pos / tile_size) / math.log(2))
        
    meta = f_new.create_dataset('meta', (1,), dtype='f')
    meta.attrs['tile-size'] = tile_size
    meta.attrs['zoom-step'] = zoom_step
    meta.attrs['max-length'] = max_pos
    meta.attrs['max-zoom'] = max_zoom

    meta.attrs['max-width'] = tile_size * 2 ** max_zoom

    prev_length = max_pos  

    min_data = da.from_array(old_data, chunks)
    max_data = da.from_array(old_data, chunks)

    for z in range(0, max_zoom, zoom_step):
        dset_length = math.ceil(max_pos / 2 ** z)

        values_dset = f_new.require_dataset('values_' + str(z), (len(old_data),), 
                             dtype='f', compression='gzip' )

        mins_dset = f_new.require_dataset('mins_' + str(z), (len(old_data),), 
                             dtype='f', compression='gzip' )
        maxs_dset = f_new.require_dataset('maxs_' + str(z), (len(old_data),), 
                             dtype='f', compression='gzip' )

        nan_values_dset = f_new.require_dataset('nan_values_' + str(z), (len(old_data),), 
                             dtype='f', compression='gzip')

        da.store(old_data, values_dset)
        da.store(min_data, mins_dset)
        da.store(max_data, maxs_dset)
        # f_new['values_' + str(z)][:] = old_data

        # see if we need to pad the end of the dataset
        # if so, use the previous last value
        if len(old_data) % zoom_factor != 0:
            old_data = da.concatenate(
                (old_data, [old_data[-1]] * ( zoom_factor - len(old_data) % zoom_factor )))
            min_data = da.concatenate(
                (min_data, [max_data[-1]] * ( zoom_factor - len(min_data) % zoom_factor )))
            max_data = da.concatenate(
                (max_data, [max_data[-1]] * ( zoom_factor - len(max_data) % zoom_factor )))

        # aggregate the data by summing adjacent datapoints
        # sys.stdout.write('summing...')
        # sys.stdout.flush()
        # print("fdsdsfs:", math.ceil(len(old_data) / zoom_factor), zoom_factor)
        # print("chunks:", chunks, zoom_factor, 'len:', len(old_data))

        old_data = old_data.rechunk(chunks)
        min_data = old_data.rechunk(chunks)
        max_data = old_data.rechunk(chunks)
        # print('zoom_factor', zoom_factor, old_data.shape)

        old_data = da.coarsen(agg_function, old_data, {0: zoom_factor})
        min_data = da.coarsen(np.min, max_data, {0: zoom_factor})
        max_data = da.coarsen(np.max, max_data, {0: zoom_factor})
        
        # reshape( (math.ceil(len(old_data) / zoom_factor), zoom_factor)).sum(axis=1)
        #sys.stdout.write(' done\n')
        #sys.stdout.flush()

        '''
        if len(old_data) < 10000:
            plt.plot(old_data)
        '''

    #plt.plot(old_data) 
    f_new.close()

def aggregate(a, num_to_agg):
    if len(a) % num_to_agg != 0:
        a = np.concatenate((a, [a[-1]] * ( num_to_agg - len(a) % num_to_agg)))

    return a.reshape((math.ceil(len(a) / num_to_agg), num_to_agg)).sum(axis=1)

def aggregate_min(a, num_to_agg):
    if len(a) % num_to_agg != 0:
        a = np.concatenate((a, [a[-1]] * ( num_to_agg - len(a) % num_to_agg)))

    return a.reshape((math.ceil(len(a) / num_to_agg), num_to_agg)).min(axis=1)

def aggregate_max(a, num_to_agg):
    if len(a) % num_to_agg != 0:
        a = np.concatenate((a, [a[-1]] * ( num_to_agg - len(a) % num_to_agg)))

    return a.reshape((math.ceil(len(a) / num_to_agg), num_to_agg)).max(axis=1)

def get_data(hdf_file, z, x):
    '''
    Return a tile from an hdf_file.

    :param hdf_file: A file handle for an HDF5 file (h5py.File('...'))
    :param z: The zoom level
    :param x: The x position of the tile
    '''

    # is the title within the range of possible tiles
    if x > 2**z:
        print("OUT OF RIGHT RANGE")
        return ([],[],[])
    if x < 0:
        print("OUT OF LEFT RANGE")
        return ([],[],[])

    d = hdf_file['meta'] 

    tile_size = int(d.attrs['tile-size'])
    zoom_step = int(d.attrs['zoom-step'])
    max_length = int(d.attrs['max-length'])
    max_zoom = int(d.attrs['max-zoom'])


    if 'min-pos' in d.attrs:
        min_pos = d.attrs['min-pos']
    else:
        min_pos = 0

    max_width = tile_size * 2 ** max_zoom

    if 'max-position' in d.attrs:
        max_position = int(d.attrs['max-position'])
    else:
        max_position = max_width

    rz = max_zoom - z
    tile_width = max_width / 2**z

    # because we only store some a subsection of the zoom levels
    next_stored_zoom = zoom_step * math.floor(rz / zoom_step)
    zoom_offset = rz - next_stored_zoom

    # the number of entries to aggregate for each new value
    num_to_agg = 2 ** zoom_offset
    total_in_length = tile_size * num_to_agg

    # which positions we need to retrieve in order to dynamically aggregate
    start_pos = int((x * 2 ** zoom_offset * tile_size))
    end_pos = int(start_pos + total_in_length)

    #print("max_position:", max_position)
    max_position = int(max_position / 2 ** next_stored_zoom)
    #print("new max_position:", max_position)


    #print("start_pos:", start_pos)
    #print("end_pos:", end_pos)
    #print("next_stored_zoom", next_stored_zoom)
    #print("max_position:", int(max_position))
    

    f = hdf_file['values_' + str(int(next_stored_zoom))]
    f_min = hdf_file['mins_' + str(int(next_stored_zoom))]
    f_max = hdf_file['maxs_' + str(int(next_stored_zoom))]


    if start_pos > max_position:
        # we want a tile that's after the last bit of data
        a = np.zeros(end_pos - start_pos)
        a.fill(np.nan)

        a_min = np.zeros(end_pos - start_pos)
        a_min.fill(np.nan)

        # umm, I don't think this needs to be here since
        # everything should be nan
        ret_array = aggregate(a, int(num_to_agg))
        min_array = aggregate_min(a_min, int(num_to_agg))
        max_array = aggregate_max(a_max, int(num_to_agg))
    elif start_pos < max_position and max_position < end_pos:
        a = f[start_pos:end_pos][:]
        a[max_position+1:end_pos] = np.nan

        a_min = f_min[start_pos:end_pos][:]
        a_min[max_position+1:end_pos] = np.nan

        a_max = f_max[start_pos:end_pos][:]
        a_max[max_position+1:end_pos] = np.nan

        ret_array = aggregate(a, int(num_to_agg))
        min_array = aggregate_min(a_min, int(num_to_agg))
        max_array = aggregate_max(a_max, int(num_to_agg))
    else:
        ret_array = aggregate(f[start_pos:end_pos], int(num_to_agg))
        min_array = aggregate_min(f_min[start_pos:end_pos], int(num_to_agg))
        max_array = aggregate_max(f_max[start_pos:end_pos], int(num_to_agg))


    #print("ret_array:", f[start_pos:end_pos])
    #print('ret_array:', ret_array)
    
    #print('nansum', np.nansum(ret_array))

    # check to see if we counted the number of NaN values in the given
    # interval

    f_nan = None
    if "nan_values_" + str(int(next_stored_zoom)) in hdf_file:
        f_nan = hdf_file['nan_values_' + str(int(next_stored_zoom))]
        nan_array = aggregate(f_nan[start_pos:end_pos], int(num_to_agg))
        num_aggregated = 2 ** (max_zoom - z)

        num_vals_array = np.zeros(len(nan_array))
        num_vals_array.fill(num_aggregated)
        num_summed_array = num_vals_array - nan_array

        averages_array = ret_array / num_summed_array

        return (averages_array, min_array, max_array)

    return (ret_array, min_array, max_array)

def tileset_info(hitile_path):
    '''
    Get the tileset info for a hitile file.

    Parameters
    ----------
    hitile_path: string
        The path to the hitile file

    Returns
    -------
    tileset_info: {'min_pos': [], 
                    'max_pos': [], 
                    'tile_size': 1024, 
                    'max_zoom': 7
                    }
    '''
    hdf_file = h5py.File(hitile_path, 'r')

    d = hdf_file['meta']

    if "min-pos" in d.attrs:
        min_pos = d.attrs['min-pos']
    else:
        min_pos = 0

    if "max-pos" in d.attrs:
        max_pos = d.attrs['max-pos']
    else:
        max_pos = d.attrs['max-length']

    return {
                "max_pos": [int(max_pos)],
                'min_pos': [int(min_pos)],
                "max_width": 2 ** math.ceil(
                    math.log(max_pos - min_pos
                    ) / math.log(2)
                ),
                "max_zoom": int(d.attrs['max-zoom']),
                "tile_size": int(d.attrs['tile-size'])
            }

def tiles(filepath, tile_ids):
    '''
    Generate tiles from a hitile file.

    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved

    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    '''
    generated_tiles = []

    for tile_id in tile_ids:
        tile_id_parts = tile_id.split('.')
        tile_position = list(map(int, tile_id_parts[1:3]))

        (dense, mins, maxs) = get_data(
            h5py.File(filepath),
            tile_position[0],
            tile_position[1]
        )

        if len(dense):
            max_dense = max(dense)
            min_dense = min(dense)
        else:
            max_dense = 0
            min_dense = 0

        min_f16 = np.finfo('float16').min
        max_f16 = np.finfo('float16').max

        has_nan = len([d for d in dense if np.isnan(d)]) > 0

        '''
        if (
            not has_nan and
            max_dense > min_f16 and max_dense < max_f16 and
            min_dense > min_f16 and min_dense < max_f16
        ):
            tile_value = {
                'dense': base64.b64encode(dense.astype('float16')).decode('utf-8'),
                'mins': base64.b64encode(mins.astype('float16')).decode('utf-8'),
                'maxs': base64.b64encode(mins.astype('float16')).decode('utf-8'),
                'dtype': 'float16'
            }
        else:
        '''
        tile_value = {
            'dense': base64.b64encode(dense.astype('float32')).decode('utf-8'),
            'mins': base64.b64encode(mins.astype('float32')).decode('utf-8'),
            'maxs': base64.b64encode(maxs.astype('float32')).decode('utf-8'),
            'dtype': 'float32'
        }

        generated_tiles += [(tile_id, tile_value)]

    return generated_tiles
