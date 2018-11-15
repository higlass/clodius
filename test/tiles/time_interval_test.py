import clodius.tiles.time_interval as hgti
import os.path as op

def test_tileset_info():
    filename = op.join('data', 'sample_htime.json')

    tsinfo = hgti.tileset_info(filename)
    # print(hgti.tileset_info(filename))
