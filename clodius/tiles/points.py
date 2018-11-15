import hgtiles.utils as hgut
import json

def tileset_info(df, x_column, y_column):
    minx = df[x_column].min()
    maxx = df[x_column].max()
    
    miny = df[y_column].min()
    maxy = df[y_column].max()
    
    return {
        'min_pos': [minx, miny],
        'max_pos': [maxx, maxy],
        'max_width': max(maxy - miny, maxx - minx),
        'max_zoom': 100
    }
    
def format_data(tiles):
    new_out = []
    
    for tile_id, tile_data in tiles:
        #print("len:", len(tile_data))
        #tile_data['data'] = tile_data['gene1']
        new_out += [(tile_id, json.loads(tile_data.to_json(orient='records')))]
        
    return new_out    

def tiles(df, x_column, y_column, tileset_info, z, x, y, width=1, height=1):
    [minx, miny, maxx, maxy] = hgut.tile_bounds(tileset_info, z, x, y, width, height)
    #print("x", x, 'y', y, width, height)
    max_per_tile = 30
    
    # get the entire region
    df = df.query('{} < {} & {} < {} & {} < {} & {} < {}'.
                    format(minx, x_column,
                          x_column, maxx,
                          miny, y_column,
                          y_column, maxy))
    
    tiles = []
    if width > 1 or height > 1:
        for i in range(width):
            for j in range(height):
                #print("i:", i, "j:", j)
                [minx, miny, maxx, maxy] = hgut.tile_bounds(tileset_info, z, x + i, y + j)

                data = df.query('{} < {} & {} < {} & {} < {} & {} < {}'.
                        format(minx, x_column,
                              x_column, maxx,
                              miny, y_column,
                              y_column, maxy))
                tiles += [((z,x+i,y+j), data[:max_per_tile])]
    else:
        return [((z,x,y), df[:max_per_tile])]
    
    return tiles
                  