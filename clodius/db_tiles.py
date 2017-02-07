import sqlite3

def get_tileset_info(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    row = c.execute("SELECT * from tileset_info").fetchone();
    tileset_info = {
            'zoom_step': row[0],
            'max_length': row[1],
            'assembly': row[2],
            'chrom_names': row[3],
            'chrom_sizes': row[4],
            'tile_size': row[5],
            'max_zoom': row[6],
            'max_width': row[7],
            "min_pos": [1],
            "max_pos": [row[1]]
            }
    conn.close()

    return tileset_info

def get_2d_tileset_info(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    row = c.execute("SELECT * from tileset_info").fetchone();
    tileset_info = {
            'zoom_step': row[0],
            'max_length': row[1],
            'assembly': row[2],
            'chrom_names': row[3],
            'chrom_sizes': row[4],
            'tile_size': row[5],
            'max_zoom': row[6],
            'max_width': row[7],
            "min_pos": [1,1],
            "max_pos": [row[1], row[1]]
            }
    conn.close()

    return tileset_info

def get_tile(db_file, zoom, tile_x_pos):
    tileset_info = get_tileset_info(db_file)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    tile_width = tileset_info['max_width'] / 2 ** zoom

    tile_start_pos = tile_width * tile_x_pos
    tile_end_pos = tile_start_pos + tile_width

    query = '''
    SELECT startPos, endPos, chrOffset, importance, fields, uid from intervals,position_index 
    where 
    intervals.id=position_index.id and zoomLevel <= {} and rEndPos >= {} and rStartPos <= {}
    '''.format(zoom, tile_start_pos, tile_end_pos)


    rows = c.execute(query).fetchall()


    # add the position offset to the returned values
    rows = [{'xStart': r[0],
             'xEnd': r[1],
             'chrOffset': r[2],
             'importance': r[3],
             'uid': r[5],
             'fields': r[4].split('\t')} for r in rows]
    conn.close()

    return rows

def get_2d_tile(db_file, zoom, tile_x_pos, tile_y_pos):
    tileset_info = get_tileset_info(db_file)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    tile_width = tileset_info['max_width'] / 2 ** zoom

    tile_x_start_pos = tile_width * tile_x_pos
    tile_x_end_pos = tile_x_start_pos + tile_width

    tile_y_start_pos = tile_width * tile_y_pos
    tile_y_end_pos = tile_y_start_pos + tile_width

    query = '''
    SELECT fromX, toX, fromY, toY, chrOffset, importance, fields, uid from intervals,position_index 
    where 
    intervals.id=position_index.id and zoomLevel <= {} and rToX >= {} and rFromX <= {} and rToY >= {} and rFromY <= {}
    '''.format(zoom, tile_x_start_pos, tile_x_end_pos, tile_y_start_pos, tile_y_end_pos)

    rows = c.execute(query).fetchall()

    # add the position offset to the returned values
    rows = [{'xStart': r[0],
             'xEnd': r[1],
             'yStart': r[2],
             'yEnd': r[3],
             'chrOffset': r[4],
             'importance': r[5],
             'uid': r[7],
             'fields': r[6].split('\t')} for r in rows]
    conn.close()

    return rows
