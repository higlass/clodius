from __future__ import print_function

import clodius.db_tiles as cdt
import clodius.cli.aggregate as cca
import os.path as op
import sys

sys.path.append("scripts")

testdir = op.realpath(op.dirname(__file__))


def test_clodius_aggregate_bedpe():
    input_file = op.join(testdir, 'sample_data', 'isidro.bedpe')
    output_file = '/tmp/isidro.bed2ddb'

    cca._bedpe(input_file, output_file, 'b37',
               importance_column=None,
               chromosome=None,
               max_per_tile=100,
               tile_size=1024,
               has_header=True)

    """
    runner = clt.CliRunner()
    result = runner.invoke(
            cca.bedpe,
            [input_file,
            '--output-file', output_file,
            '--importance-column', 'random',
            '--has-header',
            '--assembly', 'b37'])

    # print('output:', result.output, result)
    assert(result.exit_code == 0)
    """

    cdt.get_2d_tiles(output_file, 0, 0, 0)
    # print("entries:", entries)

    cdt.get_tileset_info(output_file)
    # TODO: Make assertions about result
    # print('tileset_info', tileset_info)

    cdt.get_2d_tiles(output_file, 1, 0, 0, numx=2, numy=2)
    # TODO: Make assertions about result
    # print("entries:", entries)

    cdt.get_tileset_info(output_file)
    # TODO: Make assertion


def test_clodius_aggregate_bedpe2():
    '''Use galGal6 chromsizes file'''
    input_file = op.join(testdir, 'sample_data', 'galGal6.bed')
    chromsizes_file = op.join(testdir, 'sample_data', 'galGal6.chrom.sizes')

    output_file = '/tmp/blah.bed2ddb'
    # the test is here to ensure that this doesn't raise an error
    cca._bedpe(input_file, output_file, None,
               chr1_col=1, chr2_col=1,
               from1_col=2, from2_col=2,
               to1_col=3, to2_col=3,
               importance_column=None,
               chromosome=None,
               chromsizes_filename=chromsizes_file,
               max_per_tile=100,
               tile_size=1024,
               has_header=True)
    # TODO: make assertion
