#!/usr/bin/python

import clodius.higlass_getter as chg
import clodius.save_tiles as cst
import scipy.sparse as ss
import sys
import argparse

def recursive_generate_tiles(tile_position, filepath, info, resolution):
    '''
    Recursively generate tiles from a cooler file.

    :param tile_position: A 3-tuple containing (zoom_level, x_position, y_position)
    :param filepath: The path of the cooler file
    :param info: The information about the tileset
    :param resolution: The resolution of the data in the smallest tiles (in nucleotides)
    '''
    divisor = 8
    start = 1
    zoom = 3

    print("tile_position:", tile_position)
    zoom_level = tile_position[0]
    x_pos = tile_position[1]
    y_pos = tile_position[2]

    divisor = 2 ** zoom_level

    start1 = x_pos * info['max_width'] / divisor
    end1 = (x_pos + 1) * info['max_width'] / divisor

    start2 = y_pos * info['max_width'] / divisor
    end2 = (y_pos + 1) * info['max_width'] / divisor

    print(start1, end1, start2, end2)
    data = chg.getData2(filepath, zoom_level, start1, end1, start2, end2)


    df = data
    binsize = 2 ** (info['max_zoom'] - zoom_level) * resolution


    i = (df['genome_start'].values - start1) // binsize
    j = (df['genome_end'].values - start2) // binsize
    v = df['balanced'].values
    m = (end1 - start1) // binsize
    n =  (end2 - start2) // binsize

    print(i,j,m,n)
    mat = ss.coo_matrix( (v, (i,j)), (m+1, n+1) )
    arr = mat.toarray()
    #plt.matshow(np.log10(arr), cmap='YlOrRd')

    print("arr:", arr.ravel())

    data_length = len(data)

    print("data_length:", data_length, "arr length:", len(arr.ravel()))


def main():
    parser = argparse.ArgumentParser(description="""
    python cooler_to_tiles.py cooler_file 

    Requires the cooler package.
""")

    #parser.add_argument('argument', nargs=1)
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')
    parser.add_argument('filepath')
    parser.add_argument('-e', '--elasticsearch-url', default=None,
                        help="The url of the elasticsearch database where to save the tiles")
    parser.add_argument('-b', '--bins-per-dimension', default=1,
                        help="The number of bins to consider in each dimension",
                        type=int)
    parser.add_argument('-f', '--columnfile-path', default=None,
                        help="The path to the column file where to save the tiles")
    parser.add_argument('--assembly', default=None)
    parser.add_argument('--log-file', default=None)
    parser.add_argument('--resolution', default=1000)

    args = parser.parse_args()

    num_dimensions = 2
    max_data_in_sparse = args.bins_per_dimension ** num_dimensions / 10
    if args.elasticsearch_url is not None:    
        tile_saver = cst.ElasticSearchTileSaver(max_data_in_sparse,
                                                args.bins_per_dimension,
                                                es_pat = args.elasticsearch_url,
                                                log_file = args.log_file,
                                                num_dimensions=num_dimensions)
    else:
        tile_saver = cst.ColumnFileTileSaver(max_data_in_sparse,
                                                args.bins_per_dimension,
                                                file_path = args.columnfile_path,
                                                log_file = args.log_file,
                                                num_dimensions=num_dimensions)

    ############################################################################

    info = chg.getInfo(args.filepath)
    recursive_generate_tiles((0,0,0), args.filepath, info, args.resolution)



if __name__ == '__main__':
    main()


