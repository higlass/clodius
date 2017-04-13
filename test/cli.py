from __future__ import print_function

import click.testing as clt
import clodius.cli.aggregate as cca
import h5py
import os.path as op
import sys

sys.path.append("scripts")

testdir = op.realpath(op.dirname(__file__))
def test_clodius_aggregate_bigwig():
    input_file = op.join(testdir, 'sample_data', 'values_mm9.tsv')
    runner = clt.CliRunner()
    result = runner.invoke(
            cca.tsv,
            [input_file,
            '--output-file', '/tmp/values_mm9.hitile',
            '--assembly', 'mm9'])

    print('output:', result.output, result)

    f = h5py.File('/tmp/values_mm9.hitile')
    max_zoom = f['meta'].attrs['max-zoom']

    print('max_zoom:', max_zoom)
    print("len", len(f['values_0']))

    values = f['values_0']
    
    print('values', values[:100])

    assert(result.exit_code == 0)

    '''
    print("input_file:", input_file)

    result = runner.invoke(
            cca.tsv,
            [input_file,
            '--output-file', '/tmp/eigs.hitile'])

    print('output:', result.output, result)

    assert(result.exit_code == 0)


    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'values_mm9.tsv')
    '''
