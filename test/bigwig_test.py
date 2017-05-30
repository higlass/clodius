from __future__ import print_function

import click.testing as clt
import clodius.cli.aggregate as cca
import clodius.hdf_tiles as ch
import clodius.bigWig_tiles as cbt
import h5py
import os.path as op
import sys

sys.path.append("scripts")

testdir = op.realpath(op.dirname(__file__))

def test_1d_tile():
    check_1d_file('test/sample_data/test.tile_generation.hdf5')

def check_1d_file(filename):
    f = h5py.File(filename)

    max_zoom = f['meta'].attrs['max-zoom']
    tile_size = int(f['meta'].attrs['tile-size'])

    d = ch.get_data(f, max_zoom, 0)

    # lowest zoom should have values of 1
    for i in range(tile_size):
        assert(d[i] == 1)

    d = ch.get_data(f, max_zoom-1, 0)

    for i in range(tile_size):
        assert(d[i] == 2)

    d = ch.get_data(f, max_zoom-2, 0)

    for i in range(tile_size // 2):
        assert(d[i] == 4)

    assert(d[513] == 4)

def test_tileset_info():
    check_tileset_info('test/sample_data/test.tile_generation.hdf5')

def check_tileset_info(filename):
    f = h5py.File(filename)

    ti = ch.get_tileset_info(f)

def test_clodius_aggregate_bigwig():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'test1.bw')
    print("input_file:", input_file)

    result = runner.invoke(
            cca.bigwig,
            [input_file,
            '--output-file', '/tmp/test.mr.bw'])

    print('output:', result.output, result)

    assert(result.exit_code == 0)

def test_read_bigwig():
    #ti = cbt.get_tileset_info('data/ENCFF956QEM.bigWig', 'hg19')
    import negspy.coordinates as nc

    import time
    t1 = time.time()

    i = 5
    #td = cbt.get_data('data/ENCFF956QEM.bigWig', i, 0, nc.get_chromorder('hg19'))
        #print("td:", sum(td))

    chrom_sizes = zip(nc.get_chromorder('hg19'),
                      nc.get_chromsizes('hg19'))
    print("chrom_sizes:", chrom_sizes)

    for i in range(5, 6):
        td = cbt.get_data('http://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeMapability/wgEncodeCrgMapabilityAlign75mer.bigWig', i, 0, chrom_sizes)
    t2 = time.time()
    print("elapsed:", t2 - t1)
