import clodius.tiles as ct
import math

def get_tileset_info(hdf_file):
    '''
    Get information about the tileset.

    :param hdf_file: A file handle for an HDF5 file (h5py.File('...'))
    '''
    d = hdf_file['meta']

    return {
                "max_pos": d.attrs['max-length'],
                "max_width": d.attrs['max-width'],
                "max_zoom": d.attrs['max-zoom'],
                "tile_size": d.attrs['tile-size']
            }

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
        return []
    if x < 0:
        print("OUT OF LEFT RANGE")
        return []

    d = hdf_file['meta'] 
    tile_size = int(d.attrs['tile-size'])
    zoom_step = int(d.attrs['zoom-step'])
    max_length = int(d.attrs['max-length'])
    max_zoom = int(d.attrs['max-zoom'])
    max_width = tile_size * 2 ** max_zoom

    rz = max_zoom - z
    #print("rz:", rz)
    tile_width = max_width / 2**z

    # because we only store some a subsection of the zoom levels
    next_stored_zoom = zoom_step * math.floor(rz / zoom_step)
    zoom_offset = rz - next_stored_zoom

    # the number of entries to aggregate for each new value
    num_to_agg = 2 ** zoom_offset
    total_in_length = tile_size * num_to_agg
    #print("num_to_agg:", num_to_agg, total_in_length)

    # which positions we need to retrieve in order to dynamically aggregate
    start_pos = int((x * 2 ** zoom_offset * tile_size))
    end_pos = int(start_pos + total_in_length)
    f = hdf_file['values_' + str(int(next_stored_zoom))]

    ret_array = ct.aggregate(f[start_pos:end_pos], int(num_to_agg))
    return ret_array
    
