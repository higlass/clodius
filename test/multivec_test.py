from __future__ import print_function

import click.testing as clt
import clodius.cli.aggregate as cca
import clodius.cli.convert as ccc
import clodius.db_tiles as cdt
import os
import os.path as op
import sqlite3
import sys
import tempfile

testdir = op.realpath(op.dirname(__file__))

def test_bedfile_to_multivec():
    runner = clt.CliRunner()
    input_file = op.join(testdir, 'sample_data', 'sample.bed.gz')
    f = tempfile.NamedTemporaryFile(delete=False)
    print("input_file", input_file)

    result = runner.invoke(
            ccc.bedfile_to_multivec,
            [input_file,
                '--has-header',
                '--assembly', 'hg38',
                '--base-resolution' , '10'])

    import traceback
    print("exc_info:", result.exc_info)
    a,b,tb = result.exc_info
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)

def test_load_multivec_tiles():
    input_file = op.join(testdir, 'sample_data', 'sample.bed.multires.mv5')
