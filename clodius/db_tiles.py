import collections as col
import math
import sqlite3


def get_tileset_info(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    row = c.execute("SELECT * from tileset_info").fetchone();
    if row is not None and len(row) == 9:
        header = row[8]
    else:
        header = ""

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
            "max_pos": [row[1]],
            "header": header
            }
    conn.close()

    return tileset_info


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


def get_tiles(db_file, zoom, tile_x_pos, num_tiles=1):
    '''
    Retrieve a contiguous set of tiles from a db tile file.

    Parameters
    ----------
    db_file: str
        The filename of the sqlite db file
    zoom: int
        The zoom level
    tile_x_pos: int
        The position of the first tile
    num_tiles: int
        The number of tiles to retrieve

    Returns
    -------
    tiles: {pos: tile_value}
        A set of tiles, indexed by position
    '''
    tileset_info = get_tileset_info(db_file)
    conn = sqlite3.connect(db_file)

    c = conn.cursor()

    tile_width = tileset_info['max_width'] / 2 ** zoom

    tile_start_pos = tile_width * tile_x_pos
    tile_end_pos = tile_start_pos + num_tiles * tile_width

    query = '''
    SELECT startPos, endPos, chrOffset, importance, fields, uid
    FROM intervals,position_index
    WHERE
        intervals.id=position_index.id AND
        zoomLevel <= {} AND
        rEndPos >= {} AND
        rStartPos <= {}
    '''.format(zoom, tile_start_pos, tile_end_pos)

    rows = c.execute(query).fetchall()

    new_rows = col.defaultdict(list)

    for r in rows:
        try:
            uid = r[5].decode('utf-8')
        except AttributeError:
            uid = r[5]

        tile_pos = (
            tile_x_pos + math.floor((r[0] - tile_start_pos) / tile_width)
        )

        x_start = r[0]
        x_end = r[1]

        for i in range(tile_x_pos, tile_x_pos + num_tiles):
            tile_x_start = i * tile_width
            tile_x_end = (i+1) * tile_width
            tile_pos = i

            if x_start < tile_x_end and x_end >= tile_x_start:
                new_rows[tile_pos] += [
                    # add the position offset to the returned values
                    {'xStart': r[0],
                     'xEnd': r[1],
                     'chrOffset': r[2],
                     'importance': r[3],
                     'uid': uid,
                     'fields': r[4].split('\t')}]
    conn.close()

    return new_rows


def get_2d_tiles(db_file, zoom, tile_x_pos, tile_y_pos, numx=1, numy=1):
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
    tile_y_pos: int
        The y position of the first tile
    numx: int
        The width of the block of tiles to retrieve
    numy: int
        The height of the block of tiles to retrieve

    Returns
    -------
    tiles: {pos: tile_value}
        A set of tiles, indexed by position
    '''
    tileset_info = get_tileset_info(db_file)

    conn = sqlite3.connect(db_file)

    c = conn.cursor()
    tile_width = tileset_info['max_width'] / 2 ** zoom

    tile_x_start_pos = tile_width * tile_x_pos
    tile_x_end_pos = tile_x_start_pos + (numx * tile_width)

    tile_y_start_pos = tile_width * tile_y_pos
    tile_y_end_pos = tile_y_start_pos + (numy * tile_width)

    query = '''
    SELECT
        fromX, toX, fromY, toY, chrOffset, importance, fields, uid, intervals.id
    FROM
        intervals, position_index
    WHERE
        intervals.id=position_index.id AND
        zoomLevel <= {} AND
        rToX >= {} AND
        rFromX <= {} AND
        rToY >= {} AND
        rFromY <= {}
    '''.format(
        zoom,
        tile_x_start_pos,
        tile_x_end_pos,
        tile_y_start_pos,
        tile_y_end_pos
    )

    rows = c.execute(query).fetchall()

    new_rows = col.defaultdict(list)

    for r in rows:
        try:
            uid = r[7].decode('utf-8')
        except AttributeError:
            uid = r[7]

        x_start = r[0]
        x_end = r[1]
        y_start = r[2]
        y_end = r[3]

        for i in range(tile_x_pos, tile_x_pos + numx):
            for j in range(tile_y_pos, tile_y_pos + numy):
                tile_x_start = i * tile_width
                tile_x_end = (i+1) * tile_width

                tile_y_start = j * tile_width
                tile_y_end = (j+1) * tile_width

                if (
                    x_start < tile_x_end and
                    x_end >= tile_x_start and
                    y_start < tile_y_end and
                    y_end >= tile_y_start
                ):
                    # add the position offset to the returned values
                    new_rows[(i, j)] += [
                        {'xStart': r[0],
                         'xEnd': r[1],
                         'yStart': r[2],
                         'yEnd': r[3],
                         'chrOffset': r[4],
                         'importance': r[5],
                         'uid': uid,
                         'id': r[8],
                         'fields': r[6].split('\t')}]
    conn.close()

    return new_rows
