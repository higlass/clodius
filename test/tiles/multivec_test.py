import os.path as op
import clodius.tiles.multivec as hgmu


def test_multivec():
    filename = op.join('data', 'all.KL.bed.multires.mv5')

    hgmu.tileset_info(filename)
    # TODO: Make assertions about info returned.
