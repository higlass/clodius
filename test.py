import clodius.hdf_tiles as cht
import h5py
import sys

f = h5py.File('test_chr14.hitile')
tile = cht.get_data(f, 12, 3316)
sys.exit(1)

max_zoom = 17
pos = 117440512 - 30
for i in range(max_zoom):
    tile_pos = pos / (1024 * 2 ** (max_zoom - i))
    tile = cht.get_data(f, i, tile_pos)
    print("z", i, "tile_pos:", tile_pos, "data", tile)
