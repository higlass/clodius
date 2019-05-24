import clodius.tiles.time_interval as hgti
import os.path as op


def test_tileset_info():
    filename = op.join('data', 'sample_htime.json')

    hgti.tileset_info(filename)
    # TODO: Make assertions about info returned.
