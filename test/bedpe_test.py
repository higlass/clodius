from __future__ import print_function

import clodius.db_tiles as cdt
import click.testing as clt
import clodius.cli.aggregate as cca
import h5py
import negspy.coordinates as nc
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

    #print('output:', result.output, result)
    assert(result.exit_code == 0)
    """

    entries = cdt.get_2d_tiles(output_file, 0, 0, 0)
    #print("entries:", entries)

    tileset_info = cdt.get_tileset_info(output_file)
    #print('tileset_info', tileset_info)

    entries = cdt.get_2d_tiles(output_file, 1, 0, 0, numx=2, numy=2)
    #print("entries:", entries)

    tileset_info = cdt.get_tileset_info(output_file)
    #print('tileset_info', tileset_info)
