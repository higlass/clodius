import collections as col
import itertools as it
import sqlite3

def tiles(filepath, tile_ids):
    '''
    Generate tiles from this dataset.

    Parameters
    ----------
    filepath: str
        The filename of the sqlite db file
    tile_ids: [str...]
        A list of tile ids of the form

    Returns
    -------
    tiles: [(tile_id, tile_value),...]
        A list of values indexed by the tile position
    '''
    to_return = []

    for  tile_id in tile_ids:
        parts = tile_id.split('.')
        uuid = parts[0]
        zoom = int(parts[1])
        xpos = int(parts[2])

        extra_zoom = 3
        new_rows = {}
        new_rows[xpos] = []

        for j in range(2 ** extra_zoom):
            # the old rows are indexed by the higher
            # resolution tile numbers
            higher_xpos = 2 ** extra_zoom * xpos + j
            old_rows = get_1D_tiles(filepath, 
                    zoom + extra_zoom, 
                    higher_xpos)
            new_rows[xpos] += old_rows[higher_xpos]
                
        to_return += [(tile_id, new_rows)]

    return to_return


def get_1D_tiles(db_file, zoom, tile_x_pos, numx=1, numy=1):
    '''
    Retrieve a contiguous set of tiles from a 2D db tile file.

    Parameters
    ----------
    db_file: str
        The filename of the sqlite db file
    zoom: int
        The zoom level
    tile_x_pos: int
        The x position of the first tile
    numx: int
        The width of the block of tiles to retrieve

    Returns
    -------
    tiles: {pos: tile_value}
        A set of tiles, indexed by position
    '''
    tileset_info = get_2d_tileset_info(db_file)

    conn = sqlite3.connect(db_file)

    c = conn.cursor()
    tile_width = tileset_info['max_width'] / 2 ** zoom

    tile_x_start_pos = tile_width * tile_x_pos
    tile_x_end_pos = tile_x_start_pos + (numx * tile_width)

    query = '''
    SELECT fromX, toX, fromY, toY, chrOffset, importance, fields, uid
    FROM intervals,position_index
    WHERE
        intervals.id=position_index.id AND
        zoomLevel <= {} AND
        rToX >= {} AND
        rFromX <= {} ORDER BY importance
    '''.format(
        zoom,
        tile_x_start_pos,
        tile_x_end_pos,
    )

    rows = c.execute(query).fetchall()

    query1 = '''
    SELECT fromX, toX, fromY, toY, chrOffset, importance, fields, uid
    FROM intervals,position_index
    WHERE
        intervals.id=position_index.id AND
        zoomLevel <= {} AND
        rToY >= {} AND
        rFromY <= {} ORDER BY importance
    '''.format(
        zoom,
        tile_x_start_pos,
        tile_x_end_pos,
    )

    rows1 = c.execute(query1).fetchall()

    seen_uids = set()

    new_rows = col.defaultdict(list)
    all_rows = list(sorted(it.chain(rows, rows1), key=lambda x: -x[5]))
    # print("total_len", len(all_rows))

    MAX_ROWS=25
    for r in all_rows:
        # fields = r[6].split('\t')
        #print('fields', sorted([fields[0], fields[3]]), r[5])
        try:
            uid = r[7].decode('utf-8')
        except AttributeError:
            uid = r[7]

        if uid in seen_uids:
            continue

        seen_uids.add(uid)
        if len(seen_uids) > MAX_ROWS:
            break

        x_start = r[0]
        x_end = r[1]
        y_start = r[2]
        y_end = r[3]

        for i in range(tile_x_pos, tile_x_pos + numx):
            tile_x_start = i * tile_width
            tile_x_end = (i+1) * tile_width

            if (
                (x_start < tile_x_end and
                x_end >= tile_x_start) or 
                (y_start < tile_x_end and
                    y_end >= tile_x_start)
            ):
                # add the position offset to the returned values
                new_rows[i] += [
                    {'xStart': r[0],
                     'xEnd': r[1],
                     'yStart': r[2],
                     'yEnd': r[3],
                     'chrOffset': r[4],
                     'importance': r[5],
                     'uid': uid,
                     'fields': r[6].split('\t')}]
    conn.close()

    return new_rows

def get_2d_tileset_info(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    row = c.execute("SELECT * from tileset_info").fetchone()
    tileset_info = {
        'zoom_step': row[0],
        'max_length': row[1],
        'assembly': row[2],
        'chrom_names': row[3],
        'chrom_sizes': row[4],
        'tile_size': row[5],
        'max_zoom': row[6],
        'max_width': row[7],
        'min_pos': [1, 1],
        'max_pos': [row[1], row[1]]
    }
    conn.close()

    return tileset_info
