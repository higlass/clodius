import clodius.tiles.bigbed as hgbb
import clodius.tiles.bigwig as hgbw
import os.path as op


def test_bigbed_tiles():
    filename = op.join(
        "data", "masterlist_DHSs_733samples_WM20180608_all_mean_signal_colorsMax.bed.bb"
    )

    tileset_info = hgbb.tileset_info(filename)
    num_chroms = len(tileset_info["chromsizes"])

    base_tile = hgbb.tiles(filename, ["x.0.0"])
    base_tile_data = base_tile[0][1]
    assert (num_chroms * hgbb.MAX_ELEMENTS) == len(
        base_tile_data
    ), "Number of chromosomes in bigBed file was {}\nExpected: {}\n".format(
        len(base_tile_data), (num_chroms * hgbb.MAX_ELEMENTS)
    )

    signif_tile = hgbb.tiles(filename, ["x.0.0.significant"])
    signif_tile_data = signif_tile[0][1]
    assert (num_chroms * hgbb.MAX_ELEMENTS) == len(
        signif_tile_data
    ), "Number of chromosomes in bigBed file was {}\nExpected: {}\n".format(
        len(signif_tile_data), (num_chroms * hgbb.MAX_ELEMENTS)
    )

    max_elements = [5, 10, 25, 100]
    for me in max_elements:
        max_elems_tile = hgbb.tiles(filename, ["x.0.0|max:{}".format(me)])
        max_elems_tile_data = max_elems_tile[0][1]
        for mtd in max_elems_tile_data:
            assert len(mtd) <= (
                me * hgbb.MAX_ELEMENTS
            ), "Number of observed elements with max element threshold was {}\nExpected: {}\n".format(
                len(mtd), (me * hgbb.MAX_ELEMENTS)
            )

    min_max_element_ranges = [(1, 5), (5, 10)]
    for mmer in min_max_element_ranges:
        min_element = mmer[0]
        max_element = mmer[1]
        min_max_elems_tile = hgbb.tiles(
            filename, ["x.0.0|min:{}|max:{}".format(min_element, max_element)]
        )
        min_max_elems_tile_data = min_max_elems_tile[0][1]
        mmtdl = len(min_max_elems_tile_data)
        assert (
            mmtdl >= min_element and mmtdl <= max_element * hgbb.MAX_ELEMENTS
        ), "Number of observed elements with min/max threshold was {}\nExpected: {} to {}\nAll data length: {}\nAll data: {}\n".format(
            mmtdl, min_element, max_element, mmtdl, min_max_elems_tile_data
        )


def test_tileset_info():
    filename = op.join(
        "data", "masterlist_DHSs_733samples_WM20180608_all_mean_signal_colorsMax.bed.bb"
    )

    tileset_info = hgbb.tileset_info(filename)

    assert len(tileset_info["range_modes"]) == 1
    assert tileset_info["range_modes"]["significant"]


def test_natsorted():
    chromname_tests = [
        {
            "input": ["2", "3", "4", "m", "x", "1", "y"],
            "expected": ["1", "2", "3", "4", "x", "y", "m"],
        },
        {
            "input": ["chr1", "chr4", "chr5", "chr2", "chr3", "chrMT", "chrY", "chrX"],
            "expected": [
                "chr1",
                "chr2",
                "chr3",
                "chr4",
                "chr5",
                "chrX",
                "chrY",
                "chrMT",
            ],
        },
    ]

    for test in chromname_tests:
        sorted_output = hgbw.natsorted(test["input"])
        assert (
            sorted_output == test["expected"]
        ), "Sorted output was %s\nExpected: %s" % (sorted_output, test["expected"])
