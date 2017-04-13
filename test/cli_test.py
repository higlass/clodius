from __future__ import print_function

import clodius.hdf_tiles as cht
import click.testing as clt
import clodius.cli.aggregate as cca
import h5py
import negspy.coordinates as nc
import os.path as op
import sys

sys.path.append("scripts")

testdir = op.realpath(op.dirname(__file__))
def test_clodius_aggregate_bigwig():
    input_file = op.join(testdir, 'sample_data', 'dm3_values.tsv')
    output_file = '/tmp/dm3_values.hitile'

    runner = clt.CliRunner()
    result = runner.invoke(
            cca.tsv,
            [input_file,
            '--output-file', output_file,
            '--assembly', 'dm3'])

    print('output:', result.output, result)

    f = h5py.File('/tmp/dm3_values.hitile')
    max_zoom = f['meta'].attrs['max-zoom']

    print('max_zoom:', max_zoom)
    print("len", len(f['values_0']))

    values = f['values_0']
    
    print('values', values[:100])

    # genome positions are 0 based as stored in hitile files
    assert(values[8] == 0)
    assert(values[9] == 1)
    assert(values[10] == 1)
    assert(values[13] == 1)
    assert(values[14] == 0)
    assert(values[15] == 0)

    chr_2r_pos = nc.chr_pos_to_genome_pos('chr2R', 0, 'dm3')


    assert(values[chr_2r_pos + 28] == 0)
    assert(values[chr_2r_pos + 29] == 77)
    assert(values[chr_2r_pos + 38] == 77)
    assert(values[chr_2r_pos + 39] == 0)

    assert(result.exit_code == 0)

    d = cht.get_data(f, 0, 0)
    #print("d:", d, sum(d))
    assert(sum(d) == 5 + 770)

    return

    input_file = op.join(testdir, 'sample_data', 'test3chroms_values.tsv')
    output_file = '/tmp/test3chroms_values.hitile'

    runner = clt.CliRunner()
    result = runner.invoke(
            cca.tsv,
            [input_file,
            '--output-file', output_file,
            '--assembly', 'test3chroms'])

    print('output:', result.output, result)

    f = h5py.File('/tmp/test3chroms_values.hitile')
    max_zoom = f['meta'].attrs['max-zoom']

    print('max_zoom:', max_zoom)
    print("len", len(f['values_0']))

    values = f['values_0']
    
    print('values', values[:100])

    # genome positions are 0 based as stored in hitile files
    assert(values[8] == 0)
    assert(values[9] == 1)
    assert(values[10] == 1)
    assert(values[13] == 1)
    assert(values[14] == 0)
    assert(values[15] == 0)

    chr2_pos = nc.chr_pos_to_genome_pos('chr2', 0, 'test3chroms')


    assert(values[chr2_pos + 28] == 0)
    assert(values[chr2_pos + 29] == 77)
    assert(values[chr2_pos + 38] == 77)
    assert(values[chr2_pos + 39] == 0)

    assert(result.exit_code == 0)

    d = cht.get_data(f, 0, 0)
    assert(sum(d) == 770 + 880 + 5)
    #print("d:", d)
