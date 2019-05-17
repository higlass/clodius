import clodius.tiles.files as hgfi


def test_infer_filetype():
    assert(hgfi.infer_filetype('matrix.mcool') == 'cooler')
    assert(hgfi.infer_filetype('matrix.txt') is None)
    assert(hgfi.infer_filetype('a.bigwig') == 'bigwig')
    assert(hgfi.infer_filetype('a.bigWig') == 'bigwig')


def test_infer_datatype():
    assert(hgfi.infer_datatype('cooler') == 'matrix')
    assert(hgfi.infer_datatype('bigwig') == 'vector')
