import bbi
import math
import negspy.coordinates as nc
import time

def get_tileset_info(bigWig_file, assembly):
    '''
    Get the information about the tileset that this file represents.

    Parameters
    ----------
    bigWig_file: string
        The filename of the bigWig file that will be used as a tileset

    Returns
    -------
    dictionary:
        A list of attributes that define the extent of this tileset
    '''
    bwf = pbw.open(bigWig_file)
    chrom_order = nc.get_chromorder(assembly)

    total_size = sum([bwf.chroms()[c] for c in chrom_order if c in bwf.chroms()])

    max_pos = total_size
    tile_size = 1024
    max_length = total_size
    max_zoom =  int(math.ceil(math.log(total_size / tile_size) / math.log(2)))

    print("header:", bwf.header())
    print("chroms:", bwf.chroms().keys())
    print("total_size:", total_size)
    print("max_zoom:", max_zoom)

    pass


def get_data(bigWig_file, z, x, chrom_sizes):
    '''
    Retrieve data from a bigWig file for the given zoom level.

    Parameters
    ----------
    bigWig_file: string
        The filepath of the bigwig file
    z: int
        The zoom level
    x: int
        The tile number
    chrom_sizes: [(str,int),...]
        An ordered list of tuples specifying the sizes of the individual
        chromosomes.

    Returns
    ------
    numpy array
        A 1024 element array containing the values for this tile
    '''
    '''
    chrom_sizes = bbi.chromsizes(bigWig_file)
    print("cs:", time.time() - t1)
    total_size = sum(chrom_sizes.values())
    max_pos = total_size
    tile_size = 1024
    max_length = total_size
    max_zoom =  int(math.ceil(math.log(total_size / tile_size) / math.log(2)))
    '''
    t1 = time.time()
    tile_size = 1024
    total_size = sum([x[1] for x in chrom_sizes])
    max_zoom =  int(math.ceil(math.log(total_size / tile_size) / math.log(2)))
    max_width = tile_size * 2 ** max_zoom
    tile_width = max_width / 2**z

    #print("tile_width:", tile_width)
    print("tile_size:", tile_size)
    print("tile_width:", tile_width)
    print("max_width:", max_width)

    data = []
    #data = bbi.fetch(bigWig_file, chrom_order[0], x * tile_width, (x+1) * tile_width, tile_size)
    print("cs1:", time.time() - t1)
    return data
