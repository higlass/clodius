import os.path as op

def partition_by_adjacent_tiles(tile_ids, dimension=2):
    '''
    Partition a set of tile ids into sets of adjacent tiles. For example,
    if we're requesting a set of four tiles that form a rectangle, then
    those four tiles will become one set of adjacent tiles. Non-contiguous
    tiles are not grouped together.

    Parameters
    ----------
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0.1) identifying the tiles
        to be retrieved
    dimension: int
        The dimensionality of the tiles

    Returns
    -------
    tile_lists: [tile_ids, tile_ids]
        A list of tile lists, all of which have tiles that
        are within 1 position of another tile in the list
    '''
    tile_id_lists = []

    for tile_id in sorted(tile_ids, key=lambda x: [int(p) for p in x.split('.')[2:2+dimension]]):
        tile_id_parts = tile_id.split('.')

        # exclude the zoom level in the position
        # because the tiles should already have been partitioned
        # by zoom level
        tile_position = list(map(int, tile_id_parts[2:4]))

        added = False

        for tile_id_list in tile_id_lists:
            # iterate over each group of adjacent tiles
            has_close_tile = False

            for ct_tile_id in tile_id_list:
                ct_tile_id_parts = ct_tile_id.split('.')
                ct_tile_position = list(map(int, ct_tile_id_parts[2:2+dimension]))
                far_apart = False

                # iterate over each dimension and see if this tile is close
                for p1,p2 in zip(tile_position, ct_tile_position):
                    if abs(int(p1) - int(p2)) > 1:
                        # too far apart can't be part of the same group
                        far_apart = True

                if not far_apart:
                    # no position was too far
                    tile_id_list += [tile_id]
                    added = True
                    break

            if added:
                break
        if not added:
            tile_id_lists += [[tile_id]]

    return tile_id_lists

def infer_filetype(filename):
    _,ext = op.splitext(filename)

    if ext.lower() == '.bw' or ext.lower() == '.bigwig':
        return 'bigwig'
    elif ext.lower() == '.mcool' or ext.lower() == '.cool':
        return 'cooler'
    elif ext.lower() == '.htime':
        return 'time-interval-json'
    elif ext.lower() == '.hitile':
        return 'hitile'
    elif ext.lower() == '.beddb':
        return 'beddb'
    elif ext.lower() == '.mv5':
        return 'multivec'

    return None

def infer_datatype(filetype):
    if filetype == 'cooler':
        return 'matrix'
    if filetype == 'bigwig':
        return 'vector'
    if filetype == 'time-interval-json':
        return 'time-interval'
    if filetype == 'hitile':
        return 'vector'
    if filetype == 'beddb':
        return 'bedlike'

def tiles_wrapper_2d(tile_ids, tiles_function):
    tile_values = []

    for tile_id in tile_ids:
        parts = tile_id.split('.')

        if len(parts) < 4:
            raise IndexError("Not enough tile info present")

        uid = parts[0]
        z,x,y = map(int, [parts[1], parts[2], parts[3]])

        tile_values +=  [(tile_id, 
                          tiles_function(z, x, y))]

    return tile_values

def bundled_tiles_wrapper_2d(tile_ids, tiles_function):
    '''
    Bundle adjacent tile requests so that they can be 
    processed concurrently. This is helpful for function 
    that require scanning a dataset. It's faster to filter 
    a large region and then break it down into individual 
    tiles than to go over the entire dataset and filter 
    individual tiles multiple times.
    '''
    tile_values = []

    partitioned_tile_lists = partition_by_adjacent_tiles(tile_ids)

    for tile_group in partitioned_tile_lists:
        print("tile_group:", tile_group)
        
        zoom_level = int(tile_group[0].split('.')[1])
        tileset_id = tile_group[0].split('.')[0]

        tile_positions = [[int(x) for x in t.split('.')[2:4]] for t in tile_group]

        minx = min([t[0] for t in tile_positions])
        maxx = max([t[0] for t in tile_positions])

        miny = min([t[1] for t in tile_positions])
        maxy = max([t[1] for t in tile_positions])

        tf = tiles_function(zoom_level, minx, miny, 
                width=maxx - minx + 1, height=maxy - miny + 1)

        tile_values += [("{}.{}".format(tileset_id, ".".join(map(str, tile_position))), data)
                for (tile_position, data) in tf]

    return tile_values

def random_tile(function):
        zoom_level = random.randint(0,10)
        x_pos = random.randint(0, 2 ** zoom_level)
        y_pos = random.randint(0, 2 ** zoom_level)
            
        function(hg_points, zoom_level, x_pos, y_pos)

def tile_bounds(tsinfo, z, x, y, width=1, height=1):
    '''
    Get the coordinate boundaries for the given tile.

    Parameters:
    -----------
    tsinfo: { min_pos: [], max_pos [] }
        Tileset info containing the bounds of the dataset
    z: int
        The zoom level
    x: int
        The x position
    y: int
        The y position
    width: int 
        Return bounds for a region encompassing multiple tiles 
    height: int 
        Return bounds for a region encompassing multiple tiles
    '''
    min_pos = tsinfo['min_pos']
    max_pos = tsinfo['max_pos']

    max_width = max(max_pos[0] - min_pos[0], max_pos[1] - min_pos[1])
    
    tile_width = max_width / 2 ** z
    from_x = min_pos[0] + x * tile_width
    to_x = min_pos[0] + (x+width) * tile_width
    
    from_y = min_pos[1] + y * tile_width
    to_y = min_pos[1] + (y+height) * tile_width    
    
    return [from_x, from_y, to_x, to_y]
