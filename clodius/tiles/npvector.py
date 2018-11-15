import math
import numpy as np

def tiles_wrapper(array, tile_ids):
    tile_values = []
    
    for tile_id in tile_ids:
        parts = tile_id.split('.')

        if len(parts) < 4:
            raise IndexError("Not enough tile info present")

        uid = parts[0]
        z = int(parts[1])
        x = int(parts[2])
    
        ret_array = tiles(array, z, x).reshape((-1))
        
        tile_values +=  [(tile_id, 
                         hgco.format_dense_tile(ret_array))]

    return tile_values

def tileset_info(array, bounds=None):
    '''
    Get the tileset info for the array
    '''
    bin_size = 1024
    max_dim = max(array.shape)

    max_zoom = math.ceil(math.log(max_dim / bin_size) / math.log(2))
    max_zoom = 0 if max_zoom < 0 else max_zoom

    max_width = 2 ** max_zoom * bin_size
    # print('max_zoom:', max_zoom)

    scale_up = max_width / max_dim

    if bounds:
        min_pos = [bounds[0]]
        max_pos = [bounds[1]]

        # print('scale_up:', scale_up, max_pos[0] - min_pos[0])

        max_width = (max_pos[0] - min_pos[0]) * scale_up
    else:
        min_pos = [0]
        max_pos = [array.shape[0]]
        
    
    if len(array.shape) > 1:
        raise ValueError("The array shape is not a vector type", array.shape)
    return {
        "max_width": max_width,
        "min_pos": min_pos,
        "max_pos": max_pos,
        "max_zoom": max_zoom,
        "bins_per_dimension": bin_size
    }

def tiles(array, z, x, nan_array=None, bin_size=1024):
    '''
    Return tiles at the given positions.
    
    Parameters
    -----------
    array: np.array
        An nxn array containing values
    z: int
        The zoom level (0 corresponds to most zoomed out)
    x: int
        The x tile position
    y: int
        The y tile position
    bin_size: int
        The number of values per bin
    '''
    max_dim = array.shape[0]
    #print("max_dim", max_dim)
    
    max_zoom = math.ceil(math.log(max_dim / bin_size) / math.log(2))
    max_zoom = 0 if max_zoom < 0 else max_zoom
    max_width = 2 ** max_zoom * bin_size
    
    # print("max_width:", max_width, 'bin_size:', bin_size, 'max_zoom', max_zoom)
    
    tile_width = 2 ** (max_zoom - z) * bin_size

    x_start = x * tile_width
    x_end = min(array.shape[0], x_start + tile_width)

    # print("tile_width", tile_width)
    # print("x_start:", x_start, x_end)
    
    num_to_sum = 2 ** (max_zoom - z)
    #print("num_to_sum", num_to_sum)
    
    data = array[x_start:x_end]
    #print("data:", data)
    
    # add some data so that the data can be divided into squares
    divisible_x_width = num_to_sum * math.ceil(data.shape[0] / num_to_sum)

    divisible_x_pad = divisible_x_width - data.shape[0]
    #print("data.shape", data.shape)
    
    # print("divisible_x_pad:", divisible_x_pad)
    
    a = np.pad(data, ((0, divisible_x_pad),), 'constant', 
            constant_values=(np.nan,))

    b = np.nansum(a.reshape((a.shape[0],-1,num_to_sum)),axis=2)
    ret_array = np.nansum(b.reshape(-1,num_to_sum),axis=1).astype(float)
    # print('ret_array:', ret_array)
    ret_array[ret_array == 0.] = np.nan
    #print('ret_array:', ret_array)

    #print("sum:", np.nansum(ret_array))
    
    if nan_array is not None:
        # print("normalizing")
        # we want to calculate the means of the data points
        not_nan_data = not_nan_array[x_start:x_end]
        na = np.pad(not_nan_data, ((0, divisible_x_pad)), 'constant', 
                constant_values=(np.nan,))
        nb = np.nansum(na.reshape((-1,num_to_sum)), axis=1)    
        norm_array = np.nansum(nb.reshape(-1,num_to_sum),axis=1)
        
        ret_array = ret_array / norm_array
    
    # determine how much to pad the array
    x_pad = bin_size - ret_array.shape[0]
    
    #print("ret_array:", ret_array.shape)
    #print("x_pad:", x_pad, "y_pad:", y_pad)

    return np.pad(ret_array, ((0,x_pad)), 'constant', constant_values=(np.nan, ))
