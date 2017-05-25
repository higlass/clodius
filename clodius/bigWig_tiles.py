import pyBigWig as pbw
import math
import negspy.coordinates as nc

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


def get_data(bigWig_file, z, x, chrom_order):
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
    chrom_order: [str]
        The order of the chromosomes to use when fetching data.
        Especially important when fetching data that spans multiple
        chromosomes.

    Returns
    ------
    numpy array
        A 1024 element array containing the values for this tile
    '''
    bwf = pbw.open(bigWig_file)
    total_size = sum([bwf.chroms()[c] for c in chrom_order if c in bwf.chroms()])   
    max_pos = total_size
    tile_size = 1024
    max_length = total_size
    max_zoom =  int(math.ceil(math.log(total_size / tile_size) / math.log(2)))

    max_width = tile_size * 2 ** max_zoom
    rz = max_zoom - z
    tile_width = max_width / 2**z


    #print("tile_width:", tile_width)
    print("chrom:", chrom_order[0])
    print("tile_size:", tile_size)
    print("tile_width:", tile_width)
    print(chrom_order[0], x*tile_width, (x+1) * tile_width)

    data = bwf.stats(chrom_order[0], x * tile_width, (x+1) * tile_width, type="mean", nBins=tile_size)
    return data
