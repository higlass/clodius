from __future__ import print_function

import click.testing as clt
import clodius.cli.convert as ccc
import clodius.tiles.multivec as ctv
import os.path as op
import tempfile
import h5py

testdir = op.realpath(op.dirname(__file__))


def test_bedfile_to_multivec():
    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "sample.multival.bed")
    chromsizes_file = op.join(testdir, "sample_data", "sample.multival.chrom.sizes")

    with tempfile.TemporaryDirectory() as tmp_dir:
        out_file = op.join(tmp_dir, "out.multivec")

        runner.invoke(
            ccc.bedfile_to_multivec,
            [
                input_file,
                "--output-file",
                out_file,
                "--assembly",
                "hg38",
                "--num-rows",
                3,
                "--chromsizes-filename",
                chromsizes_file,
                "--starting-resolution",
                "1000",
            ],
        )

        # import traceback
        # a, b, tb = result.exc_info

        # print("exc_info:", result.exc_info)
        # print("result:", result)
        # print("result.output", result.output)
        # print("result.error", traceback.print_tb(tb))
        # print("Exception:", a,b)

        tsinfo = ctv.tileset_info(out_file)
        # print("tsinfo:", tsinfo)

        assert "resolutions" in tsinfo
        assert tsinfo["max_pos"][0] == 18000
        tile = ctv.get_single_tile(out_file, (0, 0))

        # input_file:
        # chr1    0   1000    1.0 2.0 3.0
        # chr1    1000    2000
        # chr2    5000    6000    20.0    30.0    40.0
        #
        # # input chromsizes
        # chr1  10000
        # chr2    8000

        # first row, first chrom first value
        assert len(tile) == 3

        assert tile[0][0] == 1.0
        assert tile[0][15] == 20.0

        assert tile[1][0] == 2.0
        assert tile[2][0] == 3.0
        assert tile[2][15] == 40.0


def test_load_multivec_tiles():
    op.join(testdir, "sample_data", "sample.bed.multires.mv5")
    # TODO: Make assertions about result


def test_states_format_befile_to_multivec():
    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "states_format_input_testfile.bed.gz")
    rows_info_file = op.join(testdir, "sample_data", "states_format_test_row_infos.txt")
    tempfile.NamedTemporaryFile(delete=False)
    # TODO: Make assertions about result
    # print("input_file", input_file)

    result = runner.invoke(
        ccc.bedfile_to_multivec,
        [
            input_file,
            "--format",
            "states",
            "--row-infos-filename",
            rows_info_file,
            "--assembly",
            "hg38",
            "--starting-resolution",
            "200",
            "--num-rows",
            "10",
        ],
    )

    # import traceback
    a, b, tb = result.exc_info
    """
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    """


def test_ignore_bedfile_headers():
    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "3_header_100_testfile.bed.gz")
    rows_info_file = op.join(testdir, "sample_data", "3_header_100_row_infos.txt")
    tempfile.NamedTemporaryFile(delete=False)
    # TODO: Make assertions about result

    result = runner.invoke(
        ccc.bedfile_to_multivec,
        [
            input_file,
            "--format",
            "states",
            "--row-infos-filename",
            rows_info_file,
            "--assembly",
            "hg19",
            "--starting-resolution",
            "200",
            "--num-rows",
            "15",
        ],
    )

    # import traceback
    a, b, tb = result.exc_info


def test_retain_lines():
    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "sample2.multival.bed")
    chromsizes_file = op.join(testdir, "sample_data", "sample2.multival.chrom.sizes")
    row_infos_file = op.join(
        testdir, "sample_data", "states_format_test_row_infos_v2.txt"
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        out_file = op.join(tmp_dir, "out.multires.mv5")

        result = runner.invoke(
            ccc.bedfile_to_multivec,
            [
                input_file,
                "--output-file",
                out_file,
                "--format",
                "states",
                "--chromsizes-filename",
                chromsizes_file,
                "--starting-resolution",
                "1000",
                "--row-infos-filename",
                row_infos_file,
                "--num-rows",
                "3",
            ],
        )

        # import traceback
        a, b, tb = result.exc_info

        # input_file:
        # chr1    0   1000  State1
        # chr1    1000    10111  State2
        # chr2    5000    8000  State3
        #
        # # input chromsizes
        # chr1  10111
        # chr2    8000
        #
        # # input row_infos file
        # State1
        # State2
        # State3

        f = h5py.File(out_file, "r")
        # The last bin of chromosome 1 should contain the State2 Vector [0,1,0]
        assert f["resolutions"]["1000"]["values"]["chr1"][10][0] == 0.0
        assert f["resolutions"]["1000"]["values"]["chr1"][10][1] == 1.0
        assert f["resolutions"]["1000"]["values"]["chr1"][10][2] == 0.0


def test_chr_boundaries_states():

    data_file = op.join(testdir, "sample_data", "chrm_boundaries_test.multires.mv5")
    f = h5py.File(data_file, "r")

    chromsizes = list(zip(f["chroms"]["name"], f["chroms"]["length"]))

    # Tile that contains the boundary of chr1 and chr2 at highest resultion
    tile1 = ctv.get_tile(f, chromsizes, 200, 248934400, 248985600, [256, 4])

    assert tile1[110][0] == 1.0 and tile1[110][1] == 0.0
    assert tile1[111][0] == 0.0 and tile1[111][1] == 1.0

    # Tile that contains the boundary of chr2 and chr3 at highest resultion
    tile2 = ctv.get_tile(f, chromsizes, 200, 491110400, 491161600, [256, 4])

    assert tile2[197][0] == 0.0 and tile2[197][1] == 1.0
    assert tile2[198][0] == 1.0 and tile2[198][1] == 0.0

    # Tile that contains the boundary of chr5 and chr6 at highest resultion
    tile3 = ctv.get_tile(f, chromsizes, 200, 1061171200, 1061222400, [256, 4])

    assert tile3[135][0] == 1.0 and tile3[135][1] == 0.0
    assert tile3[136][0] == 0.0 and tile3[136][1] == 1.0
