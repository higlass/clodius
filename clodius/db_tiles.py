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
            'max_width': row[7]
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

    rows = c.execute('''
    SELECT fields from intervals,position_index 
    where 
    intervals.id=position_index.id and zoomLevel <= {} and rStartPos > {} and rEndPos < {}
    '''.format(zoom, tile_start_pos, tile_end_pos)).fetchall()


    rows = [r[0].split('\t') for r in rows]
    conn.close()

    return rows
