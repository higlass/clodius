from time import time

t1 = time()

import s3sqlite
import apsw
import logging

from clodius.utils import TILE_OPTIONS_CHAR

logger = logging.getLogger(__name__)

sovfs = s3sqlite.SmartOpenVFS(name="so-vfs")


def tileset_info(db_file):
    print("db_file", db_file)
    with apsw.Connection(
        db_file, vfs=sovfs.name, flags=apsw.SQLITE_OPEN_READONLY
    ) as conn:
        cursor = conn.cursor()

        row = cursor.execute("SELECT * from tileset_info").fetchone()

        colnames = next(zip(*cursor.description))

    if "version" not in colnames:
        version = 1
    else:
        try:
            version = int(row[colnames.index("version")])
        except ValueError:
            version = row[colnames.index("version")]

    if "header" not in colnames:
        header = ""
    else:
        header = row[colnames.index("header")]

    ts_info = {
        "zoom_step": row[0],
        "max_length": row[1],
        "assembly": row[2],
        "tile_size": row[5],
        "max_zoom": row[6],
        "max_width": row[7],
        "min_pos": [1],
        "max_pos": [row[1]],
        "header": header,
        "version": version,
        "chromsizes": list(
            zip(row[3].split("\t"), [int(cs) for cs in row[4].split("\t")])
        ),
    }
    conn.close()

    return ts_info


def tiles(filepath, tile_ids):
    """
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
    """
    to_return = []

    for tile_id in tile_ids:
        tile_option_parts = tile_id.split(TILE_OPTIONS_CHAR)[1:]
        tile_no_options = tile_id.split(TILE_OPTIONS_CHAR)[0]
        parts = tile_no_options.split(".")

        zoom = int(parts[1])
        xpos = int(parts[2])

        extra_zoom = 0
        new_rows = {}
        new_rows = []

        for j in range(2**extra_zoom):
            # the old rows are indexed by the higher
            # resolution tile numbers
            higher_xpos = 2**extra_zoom * xpos + j
            old_rows = get_1D_tiles(filepath, zoom + extra_zoom, higher_xpos)
            new_rows += old_rows

        to_return += [(tile_id, new_rows)]

    return to_return


def get_1D_tiles(db_file, zoom, tile_x_pos, num_tiles=1):
    """
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
    """
    ts_info = tileset_info(db_file)
    version = ts_info["version"]

    with apsw.Connection(
        db_file, vfs=sovfs.name, flags=apsw.SQLITE_OPEN_READONLY
    ) as conn:
        c = conn.cursor()

        tile_width = ts_info["max_width"] / 2**zoom

        tile_start_pos = tile_width * tile_x_pos
        tile_end_pos = tile_start_pos + num_tiles * tile_width

        query = """
        SELECT startPos, endPos, chrOffset, importance, fields, uid
        FROM intervals,position_index
        WHERE
            intervals.id=position_index.id AND
            zoomLevel <= {} AND
            rEndPos >= {} AND
            rStartPos <= {}
        """.format(
            zoom, tile_start_pos, tile_end_pos
        )

        if version == 2:
            query = """
            SELECT startPos, endPos, chrOffset, importance, fields, uid
            FROM intervals,position_index
            WHERE
                intervals.id=position_index.id AND
                rStartZoomLevel <= {} AND
                rEndZoomLevel >= 0 AND
                rEndPos >= {} AND
                rStartPos <= {}
            """.format(
                zoom, tile_start_pos, tile_end_pos
            )

        if version == 3:
            query = """
            SELECT startPos, endPos, chrOffset, importance, fields, uid, name
            FROM intervals,position_index
            WHERE
                intervals.id=position_index.id AND
                rStartZoomLevel <= {} AND
                rEndZoomLevel >= 0 AND
                rEndPos >= {} AND
                rStartPos <= {}
            """.format(
                zoom, tile_start_pos, tile_end_pos
            )

        if version == "3t":
            tile_id = sum([2**x for x in range(zoom)]) + tile_x_pos
            query = f"""
            SELECT startPos, endPos, chrOffset, importance, fields, uid, name
            FROM intervals, tiles
            WHERE
                tiles.id = {tile_id} AND
                tiles.intervalId = intervals.id
            """

        rows = c.execute(query).fetchall()

    new_rows = []

    for r in rows:
        try:
            uid = r[5].decode("utf-8")
        except AttributeError:
            uid = r[5]

        x_start = r[0]
        x_end = r[1]

        for i in range(tile_x_pos, tile_x_pos + num_tiles):
            tile_x_start = i * tile_width
            tile_x_end = (i + 1) * tile_width

            if x_start < tile_x_end and x_end >= tile_x_start:
                to_add = {
                    "xStart": r[0],
                    "xEnd": r[1],
                    "chrOffset": r[2],
                    "importance": r[3],
                    "uid": uid,
                    "fields": r[4].split("\t"),
                }

                if version == 3:
                    to_add["name"] = r[6]

                new_rows += [to_add]

    return new_rows


def list_items(db_file, start, end, max_entries=None):
    """
    List the entries between start and end

    Parameters
    ----------
    db_file: str
        The filename of the sqlite db file
    start_pos: int
        The start position from where to retrieve data
    end_pos: int
        The end position to get data
    max_entries: int
        The maximum number of results to return
    """
    ts_info = tileset_info(db_file)
    version = ts_info["version"]

    with apsw.Connection(
        db_file, vfs=sovfs.name, flags=apsw.SQLITE_OPEN_READONLY
    ) as conn:

        c = conn.cursor()

        # some large number because we want to extract all entries
        zoom = 100000

        query = """
        SELECT startPos, endPos, chrOffset, importance, fields, uid
        FROM intervals,position_index
        WHERE
            intervals.id=position_index.id AND
            zoomLevel <= {} AND
            rEndPos >= {} AND
            rStartPos <= {}
        """.format(
            zoom, start, end
        )

        if version == 2:
            query = """
            SELECT startPos, endPos, chrOffset, importance, fields, uid
            FROM intervals,position_index
            WHERE
                intervals.id=position_index.id AND
                rStartZoomLevel <= {} AND
                rEndZoomLevel >= 0 AND
                rEndPos >= {} AND
                rStartPos <= {}
            """.format(
                zoom, start, end
            )

        if version == 3:
            query = """
            SELECT startPos, endPos, chrOffset, importance, fields, uid, name
            FROM intervals,position_index
            WHERE
                intervals.id=position_index.id AND
                rStartZoomLevel <= {} AND
                rEndZoomLevel >= 0 AND
                rEndPos >= {} AND
                rStartPos <= {}
            """.format(
                zoom, start, end
            )
        if max_entries is not None:
            query += " LIMIT {}".format(max_entries)

        rows = c.execute(query).fetchall()

        new_rows = []

        for r in rows:
            try:
                uid = r[5].decode("utf-8")
            except AttributeError:
                uid = r[5]

            to_add = {
                "xStart": r[0],
                "xEnd": r[1],
                "chrOffset": r[2],
                "importance": r[3],
                "uid": uid,
                "fields": r[4].split("\t"),
            }

            if version == 3:
                to_add["name"] = r[6]

            new_rows += [to_add]

        return new_rows
