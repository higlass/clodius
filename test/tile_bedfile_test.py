from __future__ import print_function

import pybedtools as pbt

def test_read_bedfile():
    bt  = pbt.BedTool('test/sample_data/cnv.bed')
