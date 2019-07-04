from __future__ import print_function

import click.testing as clt
import clodius.cli.convert as ccc
import clodius.tiles.multivec as ctv
import os.path as op
import tempfile

testdir = op.realpath(op.dirname(__file__))


def test_bedfile_to_multivec():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'sample.multival.bed')
    chromsizes_file = op.join(testdir, 'sample_data', 'sample.multival.chrom.sizes')

    with tempfile.TemporaryDirectory() as tmp_dir:
        out_file = op.join(tmp_dir, 'out.multivec')

        _ = runner.invoke(
            ccc.bedfile_to_multivec,
            [input_file,
             '--output-file', out_file,
             '--assembly', 'hg38',
             '--num-rows', 3,
             '--chromsizes-filename', chromsizes_file,
             '--starting-resolution', '1000'])

        # import traceback
        # a, b, tb = result.exc_info

        # print("exc_info:", result.exc_info)
        # print("result:", result)
        # print("result.output", result.output)
        # print("result.error", traceback.print_tb(tb))
        # print("Exception:", a,b)

        tsinfo = ctv.tileset_info(out_file)
        # print("tsinfo:", tsinfo)

        assert 'resolutions' in tsinfo
        assert tsinfo['max_pos'][0] == 18000
        tile = (ctv.get_single_tile(out_file, (0, 0)))

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
    op.join(testdir, 'sample_data', 'sample.bed.multires.mv5')
    # TODO: Make assertions about result


def test_states_format_befile_to_multivec():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data',
                         'states_format_input_testfile.bed.gz')
    rows_info_file = op.join(testdir, 'sample_data',
                             'states_format_test_row_infos.txt')
    tempfile.NamedTemporaryFile(delete=False)
    # TODO: Make assertions about result
    # print("input_file", input_file)

    result = runner.invoke(
        ccc.bedfile_to_multivec,
        [input_file,
         '--format', 'states',
         '--row-infos-filename', rows_info_file,
         '--assembly', 'hg38',
         '--starting-resolution', '200',
         '--num-rows', '10'])

    # import traceback
    a, b, tb = result.exc_info
    '''
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    '''


def test_ignore_bedfile_headers():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data',
                         '3_header_100_testfile.bed.gz')
    rows_info_file = op.join(testdir, 'sample_data',
                             '3_header_100_row_infos.txt')
    tempfile.NamedTemporaryFile(delete=False)
    # TODO: Make assertions about result

    result = runner.invoke(
        ccc.bedfile_to_multivec,
        [input_file,
         '--format', 'states',
         '--row-infos-filename', rows_info_file,
         '--assembly', 'hg19',
         '--starting-resolution', '200',
         '--num-rows', '15'])

    # import traceback
    a, b, tb = result.exc_info
