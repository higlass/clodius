import math


def y2lat(a):
    return (
        180.0
        / math.pi
        * (2.0 * math.atan(math.exp(a * math.pi / 180.0)) - math.pi / 2.0)
    )


def lat2y(a):
    return (
        180.0
        / math.pi
        * math.log(math.tan(math.pi / 4.0 + a * (math.pi / 180.0) / 2.0))
    )


import clodius.tiles.geo as ctg


def tileset_info(filepath):
    tsinfo = ctg.tileset_info(filepath)
    tsinfo["min_pos"] = [-180, -180]
    tsinfo["max_pos"] = [180, 180]
    tsinfo["max_width"] = 360
    return tsinfo


def get_tiles(filepath, z, x, y, width=1, height=1):
    geo_tile = ctg.get_tiles(filepath, z, x, y, width, height)
    # print("width:", width, "height", height)
    # print("geo_tile:", geo_tile.items())
    point_tile = [
        (
            (z, x, y),
            [
                {
                    "x": u["geometry"]["coordinates"][0],
                    "y": -lat2y(u["geometry"]["coordinates"][1]),
                    "data": u["properties"]["SPECIES"],
                    "uid": u["uid"],
                }
                for u in t
            ],
        )
        for ((x, y), t) in geo_tile.items()
    ]
    return point_tile
