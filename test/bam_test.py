from __future__ import print_function

import json

import clodius.tiles.bam as ctb
import os.path as op
import unittest


class MyTestCase(unittest.TestCase):
    def test_tileset_info(self):
        filename_matched = op.join("data", "SRR1770413.sorted.short.bam")

        filename_mismatched = op.join("data", "SRR1770413.mismatched_bai.bam")

        tsinfo = ctb.tileset_info(filename_matched)
        assert "max_zoom" in tsinfo

        tsinfo = ctb.tileset_info(filename_mismatched)
        assert "max_zoom" in tsinfo

        assert "chromsizes" in tsinfo

        # the following is in here to make sure no error
        # gets thrown when dumping to JSON (e.g. from int64)
        json_str = json.dumps(tsinfo)
        assert len(json_str)

    def test_tiles(self):
        filename_matched = op.join("data", "SRR1770413.sorted.short.bam")

        filename_mismatched = op.join("data", "SRR1770413.mismatched_bai.bam")

        index_filename = op.join("data", "SRR1770413.different_index_filename.bai")

        tile = ctb.tiles(filename_matched, ["x.9.0"])

        assert len(tile) > 0

        # missing index
        self.assertRaises(ValueError, ctb.tiles, filename_mismatched, ["x.9.0"])

        tile = ctb.tiles(filename_mismatched, ["x.9.0"], index_filename=index_filename)

        assert len(tile) > 0
        # example tile: [('x.9.0', { id: [....]})]
        assert len(tile[0][1]["id"]) > 10

        tile = ctb.tiles(
            filename_mismatched,
            ["x.9.0"],
            index_filename=index_filename,
            max_tile_width=10,
        )

        assert "error" in tile[0][1]
