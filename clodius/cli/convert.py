import click
from . import cli
import clodius.chromosomes as cch
import clodius.multivec as cmv
import gzip
import h5py
import negspy.coordinates as nc
import numpy as np
import os
import os.path as op
import tempfile

import ast

def epilogos_bedline_to_vector(bedline):
    '''
    Convert a line from an epilogos bedfile to vector format.
    
    Parameters
    -----------
    parts: [string,....]
        A line from a bedfile broken up into its constituent parts
        (e.g. ["chr1", "1000", "2000", "[1,2,34,5]"])
    
    Returns
    -------
    An array containing the values associated with that line
    '''
    parts = bedline.decode('utf8').strip().split('\t')
    # extract the state values e.g. [...,[0,14],[0.56,15]]
    array_str = parts[3].split(':')[-1]

    # make sure they're ordered by their index
    array_val = sorted(ast.literal_eval(array_str), key=lambda x: x[1])
    states = [v[0] for v in array_val]

    chrom=parts[0]
    start=int(parts[1])
    end=int(parts[2])
    
    return (chrom, start, end, states)

@cli.group()
def convert():
    '''
    Aggregate a data file so that it stores the data at multiple
    resolutions.
    '''
    pass

def _bedgraph_to_multivec(
    filepath,
    output_file,
    assembly,
    chrom_col,
    from_pos_col,
    to_pos_col,
    value_col,
    has_header,
    chunk_size,
    nan_value,
    chromsizes_filename,
    starting_resolution,
    num_rows,
    format,
    row_infos_filename,
    tile_size
):

    with tempfile.TemporaryDirectory() as td:
        print('temporary dir:', td)

        temp_file = op.join(td, 'temp.mv5')
        f_out = h5py.File(temp_file, 'w')

        (chrom_info, chrom_names, chrom_sizes) = cch.load_chromsizes(chromsizes_filename, assembly)

        if row_infos_filename is not None:
            with open(row_infos_filename, 'r') as fr:
                row_infos = [l.strip().encode('utf8') for l in fr]
        else:
            row_infos = None

        for chrom in chrom_info.chrom_order:
            f_out.create_dataset(chrom, (chrom_info.chrom_lengths[chrom] // starting_resolution,
                                            num_rows),
                                            fillvalue=np.nan,
                                            compression='gzip')

        def bedline_to_chrom_start_end_vector(bedline):
            parts = bedline.strip().split()
            chrom = parts[chrom_col-1].decode('utf8')
            start = int(parts[from_pos_col-1])
            end = int(parts[to_pos_col-1])
            vector = [float(parts[value_col-1])]

            return (chrom, start, end, vector)

        if format == 'epilogos':
            cmv.bedfile_to_multivec(filepath, f_out, epilogos_bedline_to_vector, 
                    starting_resolution, has_header, chunk_size);
        else:
            cmv.bedfile_to_multivec(filepath, f_out, bedline_to_chrom_start_end_vector, 
                    starting_resolution, has_header, chunk_size);

        f_out.close()
        tf = temp_file
        f_in = h5py.File(tf, 'r')

        if output_file is None:
            output_file = op.splitext(filepath)[0] + '.multires.mv5'
        print('output_file:', output_file)

        # Override the output file if it existts
        if op.exists(output_file):
            os.remove(output_file)

        cmv.create_multivec_multires(f_in, 
                chromsizes = zip(chrom_names, chrom_sizes),
                agg=lambda x: np.nansum(x.T.reshape((x.shape[1],-1,2)),axis=2).T,
                starting_resolution=starting_resolution,
                tile_size=tile_size,
                output_file=output_file,
                row_infos=row_infos)

@convert.command()
@click.argument(
    'filepath',
    metavar='FILEPATH'
)
@click.option(
    '--output-file',
    '-o',
    default=None,
    help="The default output file name to use. If this isn't"
         "specified, clodius will replace the current extension"
         "with .hitile"
)
@click.option(
    '--assembly',
    '-a',
    help='The genome assembly that this file was created against',
    type=click.Choice(nc.available_chromsizes()),
    default='hg19'
)
@click.option(
    '--chromosome-col',
    help="The column number (1-based) which contains the chromosome "
         "name",
    default=1
)
@click.option(
    '--from-pos-col',
    help="The column number (1-based) which contains the starting "
         "position",
    default=2
)
@click.option(
    '--to-pos-col',
    help="The column number (1-based) which contains the ending"
         "position",
    default=3
)
@click.option(
    '--value-col',
    help="The column number (1-based) which contains the actual value"
         "position",
    default=4
)
@click.option(
    '--has-header/--no-header',
    help="Does this file have a header that we should ignore",
    default=False
)
@click.option(
    '--chunk-size',
    help="The size of the chunks to read in at once",
    default=1e5
)
@click.option(
    '--nan-value',
    help='The string to use as a NaN value',
    type=str,
    default=None
)
@click.option(
        '--chromsizes-filename',
        help="A file containing chromosome sizes and order",
        default=None)
@click.option(
    '--starting-resolution',
    help="The base resolution of the data. Used to determine how much space to allocate"
         " in the multivec file",
    default=1
)
@click.option(
    '--num-rows',
    help="The number of rows at each position in the multivec format",
    default=1
)
@click.option(
    '--format',
    type=click.Choice(['default', 'epilogos']),
    default='default'
    )
@click.option(
        '--row-infos-filename',
        help="A file containing the names of the rows in the multivec file",
        default=None)
@click.option(
        '--tile-size',
        '-t',
        default=256,
        help="The number of data points in each tile."
             "Used to determine the number of zoom levels"
             "to create.")
def bedfile_to_multivec(filepath, output_file, assembly, chromosome_col, 
        from_pos_col, to_pos_col, value_col, has_header, 
        chunk_size, nan_value,
        chromsizes_filename,
        starting_resolution, num_rows, 
        format, row_infos_filename, tile_size):
    _bedgraph_to_multivec(filepath, output_file, assembly, chromosome_col, 
        from_pos_col, to_pos_col, value_col, has_header, 
        chunk_size, nan_value, 
        chromsizes_filename, starting_resolution, num_rows, 
        format, row_infos_filename, tile_size)

