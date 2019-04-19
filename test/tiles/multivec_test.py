import os.path as op
import clodius.tiles.multivec as hgmu


def test_multivec():
    filename = op.join('data', 'all.KL.bed.multires.mv5')

    tsinfo = hgmu.tileset_info(filename)
    # print(hgmu.tileset_info(filename))
