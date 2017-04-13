from __future__ import print_function

import click.testing as clt
import clodius.cli.aggregate as cca
import h5py
import os.path as op
import sys

sys.path.append("scripts")

testdir = op.realpath(op.dirname(__file__))
def test_clodius_aggregate_bigwig():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'eigs.tsv')
    print("input_file:", input_file)

    result = runner.invoke(
            cca.tsv,
            [input_file,
            '--output-file', '/tmp/eigs.hitile'])

    print('output:', result.output, result)

    assert(result.exit_code == 0)
