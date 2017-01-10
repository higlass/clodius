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


    return tileset_info
