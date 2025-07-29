import os.path as op
import base64

import h5py
import numpy as np
import pytest
import clodius.tiles.multivec as hgmu
import clodius.multivec as mv


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
    filename = op.join("data", "states_format_input_testfile.100.bed.multires.mv5")

    # make sure we can retrieve the tileset info
    tsinfo = hgmu.tileset_info(filename)
    assert 10000000 in tsinfo["resolutions"]

    tiles = hgmu.tiles(filename, ["x.0.0"])
    assert "shape" in tiles[0][1]


@pytest.fixture
def zarr_sample_multivec(tmp_path):
    """Convert sample_gwas.multires.mv5 to zarr format in a temporary directory."""
    hdf5_path = op.join("test/sample_data", "sample_gwas.multires.mv5")
    zarr_path = tmp_path / "sample_gwas.multires.zarr"

    mv.hdf5_multivec_to_zarr(hdf5_path, str(zarr_path))
    return str(zarr_path)


def test_zarr_multivec_tileset_info(zarr_sample_multivec):
    """Test tileset_info works with zarr storage backend."""
    info = hgmu.tileset_info(zarr_sample_multivec, storage="zarr")

    # Compare with HDF5 version
    hdf5_path = op.join("test/sample_data", "sample_gwas.multires.mv5")
    hdf5_info = hgmu.tileset_info(hdf5_path, storage="hdf5")
    assert info["shape"] == hdf5_info["shape"]
    assert info["tile_size"] == hdf5_info["tile_size"]
    assert info["max_pos"] == hdf5_info["max_pos"]
    assert set(info["resolutions"]) == set(hdf5_info["resolutions"])


def test_zarr_multivec_get_single_tile(zarr_sample_multivec):
    """Test get_single_tile works with zarr storage backend."""
    zarr_tile = hgmu.get_single_tile(zarr_sample_multivec, [0, 0], storage="zarr")

    # Compare with HDF5 version
    hdf5_path = op.join("test/sample_data", "sample_gwas.multires.mv5")
    hdf5_tile = hgmu.get_single_tile(hdf5_path, [0, 0], storage="hdf5")
    assert zarr_tile.shape == hdf5_tile.shape
    assert np.allclose(zarr_tile, hdf5_tile, equal_nan=True)

    # Test error handling
    info = hgmu.tileset_info(zarr_sample_multivec, storage="zarr")
    with pytest.raises(IndexError):
        hgmu.get_single_tile(
            zarr_sample_multivec, [len(info["resolutions"]), 0], storage="zarr"
        )


def test_zarr_multivec_tiles(zarr_sample_multivec):
    """Test tiles function works with zarr storage backend."""
    # Get available resolutions
    info = hgmu.tileset_info(zarr_sample_multivec, storage="zarr")
    resolutions = info["resolutions"]

    # Create tile IDs for testing
    tids = [f"test_uuid.{level}.0.1231.123" for level in range(len(resolutions))]
    zarr_tiles = hgmu.tiles(zarr_sample_multivec, tids, storage="zarr")
    zarr_tiles_list = list(zarr_tiles)

    # Compare with HDF5 version
    hdf5_path = op.join("test/sample_data", "sample_gwas.multires.mv5")
    hdf5_tiles = hgmu.tiles(hdf5_path, tids, storage="hdf5")
    hdf5_tiles_list = list(hdf5_tiles)
    assert len(zarr_tiles_list) == len(hdf5_tiles_list)
    for (zarr_tile_id, zarr_tile_value), (hdf5_tile_id, hdf5_tile_value) in zip(
        zarr_tiles_list, hdf5_tiles_list
    ):
        assert zarr_tile_id == hdf5_tile_id
        assert zarr_tile_value["dense"] == hdf5_tile_value["dense"]
        assert zarr_tile_value["dtype"] == hdf5_tile_value["dtype"]


def test_zarr_multivec_consistency(zarr_sample_multivec):
    """Test that zarr tiles match single tile retrieval."""
    info = hgmu.tileset_info(zarr_sample_multivec, storage="zarr")
    resolutions = info["resolutions"]
    tids = [f"test_uuid.{level}.0.1231.123" for level in range(len(resolutions))]
    tiles = hgmu.tiles(zarr_sample_multivec, tids, storage="zarr")
    for tile_id, tile_value in tiles:
        tile_pos = [int(i) for i in tile_id.split(".")[1:3]]
        single_tile = hgmu.get_single_tile(
            zarr_sample_multivec, tile_pos, storage="zarr"
        ).astype(tile_value["dtype"])
        assert (
            base64.b64encode(single_tile.ravel()).decode("utf-8") == tile_value["dense"]
        )
