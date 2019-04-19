import math


def get_tile_box(zoom, x, y):
    """convert Google-style Mercator tile coordinate to
    (minlat, maxlat, minlng, maxlng) bounding box"""

    minlng, minlat = get_lng_lat_from_tile_pos(zoom, x, y)
    maxlng, maxlat = get_lng_lat_from_tile_pos(zoom, x + 1, y + 1)

    return (minlng, maxlng, minlat, maxlat)


def get_lng_lat_from_tile_pos(zoom, x, y):
    """convert Google-style Mercator tile coordinate to
    (lng, lat) of top-left corner of tile"""

    # "map-centric" latitude, in radians:
    lat_rad = math.pi - 2 * math.pi * y / (2**zoom)
    # true latitude:
    lat_rad = gudermannian(lat_rad)
    lat = lat_rad * 180.0 / math.pi

    # longitude maps linearly to map, so we simply scale:
    lng = -180.0 + 360.0 * x / (2**zoom)

    return (lng, lat)


def get_tile_pos_from_lng_lat(lng, lat, zoom):
    """convert lng/lat to Google-style Mercator tile coordinate (x, y)
    at the given zoom level"""

    lat_rad = lat * math.pi / 180.0
    # "map-centric" latitude, in radians:
    lat_rad = inv_gudermannian(lat_rad)

    x = 2**zoom * (lng + 180.0) / 360.0
    y = 2**zoom * (math.pi - lat_rad) / (2 * math.pi)

    return (x, y)


def gudermannian(x):
    return 2 * math.atan(math.exp(x)) - math.pi / 2


def inv_gudermannian(y):
    return math.log(math.tan((y + math.pi / 2) / 2))
