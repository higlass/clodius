import clodius.tiles.bigwig as hgbi
import os.path as op
import numpy as np
import base64


def test_bigwig_tiles():
    filename = op.join(
        "data", "wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig"
    )

    mean_tile = hgbi.tiles(filename, ["x.0.0"])
    mean_mean_tile = hgbi.tiles(filename, ["x.0.0.mean"])
    min_tile = hgbi.tiles(filename, ["x.0.0.min"])
    max_tile = hgbi.tiles(filename, ["x.0.0.max"])
    std_tile = hgbi.tiles(filename, ["x.0.0.std"])

    assert mean_tile[0][1]["max_value"] == mean_mean_tile[0][1]["max_value"]
    assert mean_tile[0][1]["max_value"] > min_tile[0][1]["max_value"]
    assert max_tile[0][1]["max_value"] > mean_tile[0][1]["max_value"]
    assert (
        max_tile[0][1]["max_value"]
        > mean_tile[0][1]["max_value"] + std_tile[0][1]["max_value"]
    )

    min_max_tile = hgbi.tiles(filename, ["x.0.0.minMax"])
    whisker_tile = hgbi.tiles(filename, ["x.0.0.whisker"])

    mean_val = np.frombuffer(
        base64.b64decode(mean_tile[0][1]["dense"]), dtype=mean_tile[0][1]["dtype"]
    )

    min_val = np.frombuffer(
        base64.b64decode(min_tile[0][1]["dense"]), dtype=min_tile[0][1]["dtype"]
    )

    max_val = np.frombuffer(
        base64.b64decode(max_tile[0][1]["dense"]), dtype=max_tile[0][1]["dtype"]
    )

    std_val = np.frombuffer(
        base64.b64decode(std_tile[0][1]["dense"]), dtype=std_tile[0][1]["dtype"]
    )

    min_max_val = np.frombuffer(
        base64.b64decode(min_max_tile[0][1]["dense"]), dtype=min_max_tile[0][1]["dtype"]
    )

    mean_tile = hgbi.tiles(filename, ["x.0.0"])
    mean_mean_tile = hgbi.tiles(filename, ["x.0.0.mean"])
    min_tile = hgbi.tiles(filename, ["x.0.0.min"])
    max_tile = hgbi.tiles(filename, ["x.0.0.max"])
    std_tile = hgbi.tiles(filename, ["x.0.0.std"])

    assert mean_tile[0][1]["max_value"] == mean_mean_tile[0][1]["max_value"]
    assert mean_tile[0][1]["max_value"] > min_tile[0][1]["max_value"]
    assert max_tile[0][1]["max_value"] > mean_tile[0][1]["max_value"]
    assert (
        max_tile[0][1]["max_value"]
        > mean_tile[0][1]["max_value"] + std_tile[0][1]["max_value"]
    )

    min_max_tile = hgbi.tiles(filename, ["x.0.0.minMax"])
    whisker_tile = hgbi.tiles(filename, ["x.0.0.whisker"])

    mean_val = np.frombuffer(
        base64.b64decode(mean_tile[0][1]["dense"]), dtype=mean_tile[0][1]["dtype"]
    )

    min_val = np.frombuffer(
        base64.b64decode(min_tile[0][1]["dense"]), dtype=min_tile[0][1]["dtype"]
    )

    max_val = np.frombuffer(
        base64.b64decode(max_tile[0][1]["dense"]), dtype=max_tile[0][1]["dtype"]
    )

    std_val = np.frombuffer(
        base64.b64decode(std_tile[0][1]["dense"]), dtype=std_tile[0][1]["dtype"]
    )

    min_max_val = np.frombuffer(
        base64.b64decode(min_max_tile[0][1]["dense"]), dtype=min_max_tile[0][1]["dtype"]
    )

    whisker_val = np.frombuffer(
        base64.b64decode(whisker_tile[0][1]["dense"]), dtype=whisker_tile[0][1]["dtype"]
    )

    assert min_max_val.shape[0] == 2 * mean_val.shape[0]
    assert np.allclose(min_max_val[::2], min_val, equal_nan=True)
    assert np.allclose(min_max_val[1::2], max_val, equal_nan=True)

    assert whisker_val.shape[0] == 4 * mean_val.shape[0]
    assert np.allclose(whisker_val[::4], min_val, equal_nan=True)
    assert np.allclose(whisker_val[1::4], max_val, equal_nan=True)
    assert np.allclose(whisker_val[2::4], mean_val, equal_nan=True)
    assert np.allclose(whisker_val[3::4], std_val, equal_nan=True)


def test_tileset_info():
    filename = op.join(
        "data", "wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig"
    )

    tileset_info = hgbi.tileset_info(filename)

    assert len(tileset_info["aggregation_modes"]) == 5

    assert "mean" in [m["value"] for m in tileset_info["aggregation_modes"]]
    assert "min" in [m["value"] for m in tileset_info["aggregation_modes"]]
    assert "max" in [m["value"] for m in tileset_info["aggregation_modes"]]
    assert "std" in [m["value"] for m in tileset_info["aggregation_modes"]]

    assert len(tileset_info["range_modes"]) == 2
    assert "minMax" in [m["value"] for m in tileset_info["range_modes"]]
    assert "whisker" in [m["value"] for m in tileset_info["range_modes"]]


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
        sorted_output = hgbi.natsorted(test["input"])
        assert (
            sorted_output == test["expected"]
        ), "Sorted output was %s\nExpected: %s" % (sorted_output, test["expected"])
