import os.path as op
import base64

import h5py
import pytest
import clodius.tiles.multivec as hgmu


def test_multivec():
    filename = op.join("test/sample_data", "sample_gwas.multires.mv5")
    with h5py.File(filename, "r") as h5:
        tile_size = h5["info"].attrs["tile-size"]
        resolutions = list(h5["resolutions"].keys())
        reso = resolutions[0]
        chroms = h5[f"resolutions/{reso}/chroms/name"][:]
        num_rows = h5[f"resolutions/{reso}/values"][chroms[0]].shape[1]
        total_length = sum(h5["chroms/length"])
    # info
    info = hgmu.tileset_info(filename)
    assert info["shape"] == [tile_size, num_rows]
    assert info["tile_size"] == tile_size
    assert info["max_pos"] == total_length
    assert set(info["resolutions"]) == set(int(reso) for reso in resolutions)

    # get_single_tile
    test_tile = hgmu.get_single_tile(filename, [0, 0])
    assert list(test_tile.shape)[::-1] == info["shape"]
    with pytest.raises(IndexError):
        hgmu.get_single_tile(filename, [len(resolutions), 0])

    # tiles
    tids = [f"test_uuid.{level}.0.1231.123" for level in range(len(resolutions))]
    tiles = hgmu.tiles(filename, tids)
    for tile_id, tile_value in tiles:
        tile_pos = [int(i) for i in tile_id.split(".")[1:3]]
        single_tile = hgmu.get_single_tile(filename, tile_pos).astype(
            tile_value["dtype"]
        )
        assert (
            base64.b64encode(single_tile.ravel()).decode("utf-8") == tile_value["dense"]
        )


def test_states():
    filename = op.join(
        "data", "states_format_input_testfile.100.bed.multires.mv5"
    )

    # make sure we can retrieve the tileset info
    tsinfo = hgmu.tileset_info(filename)
    assert 10000000 in tsinfo["resolutions"]

    tiles = hgmu.tiles(filename, ["x.0.0"])
    assert "shape" in tiles[0][1]
