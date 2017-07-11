from __future__ import print_function

import clodius.db_tiles as cdt
import clodius.hdf_tiles as cht
import click.testing as clt
import clodius.cli.aggregate as cca
import h5py
import negspy.coordinates as nc
import numpy as np
import os.path as op
import sys

sys.path.append("scripts")

testdir = op.realpath(op.dirname(__file__))
def test_clodius_aggregate_bedgraph():
    input_file = op.join(testdir, 'sample_data', 'cnvs_hw.tsv')
    output_file = '/tmp/cnvs_hw.hitile'

    runner = clt.CliRunner()
    result = runner.invoke(
            cca.bedgraph,
            [input_file,
            '--output-file', output_file,
            '--assembly', 'grch37',
            '--chromosome-col', '2',
            '--from-pos-col', '3',
            '--to-pos-col', '4',
            '--value-col', '5',
            '--has-header',
            '--nan-value', 'NA'])

    '''
    import traceback
    print("exc_info:", result.exc_info)
    a,b,tb = result.exc_info
    print("result:", result)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    '''
    f = h5py.File(output_file)
    #print("tile_0_0", d)

    #print("tile:", cht.get_data(f, 22, 0))
    #return
    d = cht.get_data(f,0,0)

    assert(not np.isnan(d[0]))
    assert(np.isnan(d[-1]))
    prev_tile_3_0 = cht.get_data(f,3,0)

    print("prev_tile_3_0:", prev_tile_3_0)

    assert(result.exit_code == 0)
    return
    assert(sum(prev_tile_3_0) < 0)

    input_file = op.join(testdir, 'sample_data', 'cnvs_hw.tsv.gz')
    result = runner.invoke(
            cca.bedgraph,
            [input_file,
            '--output-file', output_file,
            '--assembly', 'grch37',
            '--chromosome-col', '2',
            '--from-pos-col', '3',
            '--to-pos-col', '4',
            '--value-col', '5',
            '--has-header',
            '--nan-value', 'NA'])

    '''
    import traceback
    print("exc_info:", result.exc_info)
    a,b,tb = result.exc_info
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    '''

    f = h5py.File(output_file)
    tile_3_0 = cht.get_data(f,3,0)


    assert(sum(tile_3_0) - sum(prev_tile_3_0) < 0.0001)

testdir = op.realpath(op.dirname(__file__))
def test_clodius_aggregate_bedpe():
    input_file = op.join(testdir, 'sample_data', 'Rao_RepA_GM12878_Arrowhead.txt')
    output_file = '/tmp/bedpe.db'

    runner = clt.CliRunner()
    result = runner.invoke(
            cca.bedpe,
            [input_file,
            '--output-file', output_file,
            '--assembly', 'hg19',
            '--chr1-col', '1',
            '--from1-col', '2',
            '--to1-col', '3',
            '--chr2-col', '1',
            '--from2-col', '2',
            '--to2-col', '3'])
    print("result:", result)
    print("result.output", result.output)

    tiles = cdt.get_2d_tile(output_file, 0,0,0)
    print("tiles:", tiles)


testdir = op.realpath(op.dirname(__file__))
def test_clodius_aggregate_bigwig():
    input_file = op.join(testdir, 'sample_data', 'dm3_values.tsv')
    output_file = '/tmp/dm3_values.hitile'

    runner = clt.CliRunner()
    result = runner.invoke(
            cca.bedgraph,
            [input_file,
            '--output-file', output_file,
            '--assembly', 'dm3'])

    f = h5py.File('/tmp/dm3_values.hitile')
    max_zoom = f['meta'].attrs['max-zoom']
    values = f['values_0']
    
    import numpy as np
    print("values:", values[8])
    # genome positions are 0 based as stored in hitile files
    assert(np.isnan(values[8]))
    assert(values[9] == 1)
    assert(values[10] == 1)
    assert(values[13] == 1)
    assert(np.isnan(values[14]))
    assert(np.isnan(values[15]))

    chr_2r_pos = nc.chr_pos_to_genome_pos('chr2R', 0, 'dm3')


    assert(np.isnan(values[chr_2r_pos + 28]))
    assert(values[chr_2r_pos + 29] == 77)
    assert(values[chr_2r_pos + 38] == 77)
    assert(values[chr_2r_pos + 39] == 0)

    assert(result.exit_code == 0)

    d = cht.get_data(f, 0, 0)
    assert(sum(d) == 5 + 770)

    return

    input_file = op.join(testdir, 'sample_data', 'test3chroms_values.tsv')
    output_file = '/tmp/test3chroms_values.hitile'

    runner = clt.CliRunner()
    result = runner.invoke(
            cca.bedgraph,
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
