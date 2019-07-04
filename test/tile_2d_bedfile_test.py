from __future__ import print_function

import clodius.db_tiles as cdt


def test_get_tileset_info():
    filename = 'test/sample_data/arrowhead_domains_short.txt.multires.db'
    t = cdt.get_tileset_info(filename)

    assert(t['zoom_step'] == 1)
    assert(t['max_length'] == 3095693981)
    assert(t['max_width'] > 4000000000)
    assert(t['max_width'] < 5000000000)


def test_get_tiles():
    filename = 'test/sample_data/arrowhead_domains_short.txt.multires.db'

    cdt.get_2d_tiles(filename, 0, 0, 0, numx=1, numy=1)
    # TODO: Make an assertion
