import math
import numpy as np
import clodius.tiles.format as ctf
import clodius.tiles.npvector as ctn

import json

def tiles(array, z, x, importances, tile_size=16):
    '''
    Return tiles from the array. If importances are provided,
    then they will be used to prioritize entries. Otherwise we'll
    assign random importances to the entire array.
    '''
    max_zoom, x_start, x_end = ctn.max_zoom_and_data_bounds(array, z, x, tile_size)
    # print("XXXX x_start:x_end", x_start, x_end)
    
    tile_array = array[x_start:x_end]
    tile_importances = importances[x_start:x_end]

    indeces = np.argsort(tile_importances)[::-1][:tile_size]
    
    return [{'x': x, 'label': label, 'importance': importance} for x, label, importance 
        in zip([int(i) for i in x_start + indeces], tile_array[indeces], tile_importances[indeces] )]

def tiles_wrapper(array, tile_ids, importances):
    tile_values = []
    
    for tile_id in tile_ids:
        parts = tile_id.split('.')

        if len(parts) < 3:
            raise IndexError("Not enough tile info present")

        uid = parts[0]
        z = int(parts[1])
        x = int(parts[2])
    
        ret_array = tiles(array, z, x, importances)
        
        tile_values +=  [(tile_id, ret_array)]

    return tile_values