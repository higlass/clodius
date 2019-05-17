import base64
import os
import sqlite3


def get_tileset_info(tileset):
    if not os.path.isfile(tileset):
        return {
            'error': 'Tileset info is not available!'
        }

    db = sqlite3.connect(tileset)

    res = db.execute('SELECT * FROM tileset_info').fetchone()

    o = {
        'tile_size': res[5],
        'max_zoom': res[6],
        'max_size': res[7],
    }

    try:
        o['width'] = res[8]
        o['height'] = res[9]
    except IndexError:
        pass

    try:
        o['dtype'] = res[10]
    except IndexError:
        pass

    return o


def get_tiles(filename, tile_ids, raw):
    '''
    Generate tiles from a imtiles file.
    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0.1) identifying the tiles
        to be retrieved
    Returns
    -------
    generated_tiles: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples
    '''
    # Connect to SQLite db
    db = sqlite3.connect(filename)

    generate_tiles = []

    generate_image = raw and len(tile_ids)

    for tile_id in tile_ids:
        id = tile_id[tile_id.find('.') + 1:].split('.')

        sql = 'SELECT image FROM tiles WHERE z = :z AND y = :y AND x = :x'
        param = {'z': int(id[0]), 'y': int(id[1]), 'x': int(id[2])}
        res = db.execute(sql, param).fetchone()

        if res:
            image_blob = res[0]

            if generate_image:
                tile_data = {
                    'image': image_blob,
                }
            else:
                tile_data = {
                    'dense': base64.b64encode(image_blob).decode('latin-1'),
                }

            generate_tiles.append((tile_id, tile_data))

    return generate_tiles
