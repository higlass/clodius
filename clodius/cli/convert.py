# -*- coding: utf-8 -*-
from __future__ import division, print_function

from . import cli

@cli.group()
def convert():
    '''
    Aggregate a data file so that it stores the data at multiple
    resolutions.
    '''
    pass

###############################################################################
def _bedgraph(
    filepath,
    output_file,
    assembly,
    chrom_col,
    from_pos_col,
    to_pos_col,
    value_col,
    has_header,
    chromosome,
    tile_size,
    chunk_size,
    method,
    nan_value,
    transform,
    count_nan,
    chromsizes_filename,
    zoom_step
):
    if output_file is None:
        output_file = op.splitext(filepath)[0] + '.hitile'

    print("output file:", output_file)

    # Override the output file if it existts
    if op.exists(output_file):
        os.remove(output_file)
    f = h5py.File(output_file, 'w')

    # get the information about the chromosomes in this assembly
    if chromsizes_filename is not None:
        chrom_info = nc.get_chrominfo_from_file(chromsizes_filename)
        chrom_order = [
            a.encode('utf-8')
            for a
            in nc.get_chromorder_from_file(chromsizes_filename)
        ]
        chrom_sizes = nc.get_chromsizes_from_file(chromsizes_filename)
    else:
        chrom_info = nc.get_chrominfo(assembly)
        chrom_order = [a.encode('utf-8') for a in nc.get_chromorder(assembly)]
        chrom_sizes = nc.get_chromsizes(assembly)

    assembly_size = chrom_info.total_length
    print('assembly_size:', assembly_size)

    tile_size = tile_size
    # how many values to read in at once while tiling
    chunk_size = tile_size * 2 ** chunk_size

    dsets = []  # data sets at each zoom level
    nan_dsets = []  # store nan values

    # initialize the arrays which will store the values at each stored zoom
    # level
    z = 0
    positions = []   # store where we are at the current dataset
    data_buffers = [[]]
    nan_data_buffers = [[]]

    while assembly_size / 2 ** z > tile_size:
        dset_length = math.ceil(assembly_size / 2 ** z)
        dsets += [
            f.create_dataset(
                'values_' + str(z),
                (dset_length,),
                dtype='f',
                compression='gzip'
            )
        ]
        nan_dsets += [
            f.create_dataset(
                'nan_values_' + str(z),
                (dset_length,),
                dtype='f',
                compression='gzip'
            )
        ]

        data_buffers += [[]]
        nan_data_buffers += [[]]

        positions += [0]
        z += zoom_step

    # store some meta data
    d = f.create_dataset('meta', (1,), dtype='f')

    print("assembly:", assembly)

    d.attrs['zoom-step'] = zoom_step
    d.attrs['max-length'] = assembly_size
    d.attrs['assembly'] = assembly
    d.attrs['chrom-names'] = chrom_order
    d.attrs['chrom-sizes'] = chrom_sizes
    d.attrs['chrom-order'] = chrom_order
    d.attrs['tile-size'] = tile_size
    d.attrs['max-zoom'] = max_zoom = math.ceil(
        math.log(d.attrs['max-length'] / tile_size) / math.log(2)
    )
    d.attrs['max-width'] = tile_size * 2 ** max_zoom
    d.attrs['max-position'] = 0

    print("assembly size (max-length)", d.attrs['max-length'])
    print("max-width", d.attrs['max-width'])
    print("max_zoom:", d.attrs['max-zoom'])
    print("chunk-size:", chunk_size)
    print("chrom-order", d.attrs['chrom-order'])

    t1 = time.time()

    # are we reading the input from stdin or from a file?

    if filepath == '-':
        f = sys.stdin
    else:
        if filepath.endswith('.gz'):
            import gzip
            f = gzip.open(filepath, 'rt')
        else:
            f = open(filepath, 'r')

    curr_zoom = 0

    def add_values_to_data_buffers(buffers_to_add, nan_buffers_to_add):
        curr_zoom = 0

        data_buffers[0] += buffers_to_add
        nan_data_buffers[0] += nan_buffers_to_add

        curr_time = time.time() - t1
        percent_progress = (positions[curr_zoom] + 1) / float(assembly_size)
        print(
            "position: {} progress: {:.2f} elapsed: {:.2f} "
            "remaining: {:.2f}".format(
                positions[curr_zoom] + 1,
                percent_progress,
                curr_time, curr_time / (percent_progress) - curr_time
            )
        )

        while len(data_buffers[curr_zoom]) >= chunk_size:
            # get the current chunk and store it, converting nans to 0
            print("len(data_buffers[curr_zoom])", len(data_buffers[curr_zoom]))
            curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
            nan_curr_chunk = np.array(nan_data_buffers[curr_zoom][:chunk_size])

            '''
            print("1cc:", sum(curr_chunk))
            print("1db:", data_buffers[curr_zoom][:chunk_size])
            print("1curr_chunk:", nan_curr_chunk)
            '''
            print("positions[curr_zoom]:", positions[curr_zoom])

            curr_pos = positions[curr_zoom]
            dsets[curr_zoom][curr_pos:curr_pos+chunk_size] = curr_chunk
            nan_dsets[curr_zoom][curr_pos:curr_pos+chunk_size] = nan_curr_chunk

            # aggregate and store aggregated values in the next zoom_level's
            # data
            data_buffers[curr_zoom+1] += list(
                ct.aggregate(curr_chunk, 2 ** zoom_step)
            )
            nan_data_buffers[curr_zoom+1] += list(
                ct.aggregate(nan_curr_chunk, 2 ** zoom_step)
            )

            data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
            nan_data_buffers[curr_zoom] =\
                nan_data_buffers[curr_zoom][chunk_size:]

            # data = data_buffers[curr_zoom+1]
            # nan_data = nan_data_buffers[curr_zoom+1]

            # do the same for the nan values buffers

            positions[curr_zoom] += chunk_size
            curr_zoom += 1

            if curr_zoom * zoom_step >= max_zoom:
                break

    values = []
    nan_values = []

    if has_header:
        f.readline()

    # the genome position up to which we've filled in values
    curr_genome_pos = 0

    # keep track of the previous value so that we can use it to fill in NAN
    # values
    # prev_value = 0

    for line in f:
        # each line should indicate a chromsome, start position and end
        # position
        parts = line.strip().split()

        start_genome_pos = (
            chrom_info.cum_chrom_lengths[parts[chrom_col-1]] +
            int(parts[from_pos_col-1])
        )

        if start_genome_pos - curr_genome_pos > 1:
            values += [np.nan] * (start_genome_pos - curr_genome_pos - 1)
            nan_values += [1] * (start_genome_pos - curr_genome_pos - 1)

            curr_genome_pos += (start_genome_pos - curr_genome_pos - 1)

        # count how many nan values there are in the dataset
        nan_count = 1 if parts[value_col-1] == nan_value else 0

        # if the provided values are log2 transformed, we have to un-transform
        # them
        if transform == 'exp2':
            value = (
                2 ** float(parts[value_col-1])
                if not parts[value_col-1] == nan_value
                else np.nan
            )
        else:
            value = (
                float(parts[value_col-1])
                if not parts[value_col-1] == nan_value
                else np.nan
            )


        # we're going to add as many values are as specified in the bedfile line
        values_to_add = [value] * (int(parts[to_pos_col-1]) - int(parts[from_pos_col-1]))
        nan_counts_to_add = [nan_count] * (int(parts[to_pos_col-1]) - int(parts[from_pos_col-1]))

        if closed_interval:
            values_to_add += [value]
            nan_counts_to_add += [nan_count]

        # print("values_to_add", values_to_add)
        
        values += values_to_add
        nan_values += nan_counts_to_add

        d.attrs['max-position'] = start_genome_pos + len(values_to_add)

        curr_genome_pos += len(values_to_add)

        while len(values) > chunk_size:
            print("len(values):", len(values), chunk_size)
            print("line:", line)
            add_values_to_data_buffers(
                values[:chunk_size], nan_values[:chunk_size]
            )
            values = values[chunk_size:]
            nan_values = nan_values[chunk_size:]

    add_values_to_data_buffers(values, nan_values)

    # store the remaining data
    while True:
        # get the current chunk and store it
        chunk_size = len(data_buffers[curr_zoom])
        curr_chunk = np.array(data_buffers[curr_zoom][:chunk_size])
        nan_curr_chunk = np.array(nan_data_buffers[curr_zoom][:chunk_size])

        '''
        print("2curr_chunk", curr_chunk)
        print("2curr_zoom:", curr_zoom)
        print("2db", data_buffers[curr_zoom][:100])
        '''

        curr_pos = positions[curr_zoom]
        dsets[curr_zoom][curr_pos:curr_pos+chunk_size] = curr_chunk
        nan_dsets[curr_zoom][curr_pos:curr_pos+chunk_size] = nan_curr_chunk

        # aggregate and store aggregated values in the next zoom_level's data
        data_buffers[curr_zoom+1] += list(
            ct.aggregate(curr_chunk, 2 ** zoom_step)
        )
        nan_data_buffers[curr_zoom+1] += list(
            ct.aggregate(nan_curr_chunk, 2 ** zoom_step)
        )

        data_buffers[curr_zoom] = data_buffers[curr_zoom][chunk_size:]
        nan_data_buffers[curr_zoom] = nan_data_buffers[curr_zoom][chunk_size:]

        # data = data_buffers[curr_zoom+1]
        # nan_data = nan_data_buffers[curr_zoom+1]

        positions[curr_zoom] += chunk_size
        curr_zoom += 1

        # we've created enough tile levels to cover the entire maximum width
        if curr_zoom * zoom_step >= max_zoom:
            break

    # still need to take care of the last chunk

@aggregate.command()
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
    '--chromosome',
    default=None,
    help="Only extract values for a particular chromosome."
         "Use all chromosomes if not set."
)
@click.option(
    '--tile-size',
    '-t',
    default=1024,
    help="The number of data points in each tile."
         "Used to determine the number of zoom levels"
         "to create."
)
@click.option(
    '--chunk-size',
    '-c',
    help='How many values to aggregate at once.'
         'Specified as a power of two multiplier of the tile'
         'size',
    default=14
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
    '--method',
    help='The method used to aggregate values (e.g. sum, average...)',
    type=click.Choice(['sum', 'average']),
    default='sum'
)
@click.option(
    '--nan-value',
    help='The string to use as a NaN value',
    type=str,
    default=None
)
@click.option(
    '--transform',
    help='The method used to aggregate values (e.g. sum, average...)',
    type=click.Choice(['none', 'exp2']),
    default='none'
)
@click.option(
    '--count-nan',
    help="Simply count the number of nan values in the file",
    is_flag=True
)
@click.option(
        '--closed-interval',
        help="Treat the to column as a closed interval",
        is_flag=True)
@click.option(
        '--chromsizes-filename',
        help="A file containing chromosome sizes and order",
        default=None)
@click.option(
        '--zoom-step',
        '-z',
        help="The number of intermediate aggregation levels to"
             "omit",
        default=8)
def bedgraph(filepath, output_file, assembly, chromosome_col, 
        from_pos_col, to_pos_col, value_col, has_header, 
        chromosome, tile_size, chunk_size, method, nan_value, 
        transform, count_nan, closed_interval,
        chromsizes_filename, zoom_step):
    _bedgraph(filepath, output_file, assembly, chromosome_col, 
        from_pos_col, to_pos_col, value_col, has_header, 
        chromosome, tile_size, chunk_size, method, nan_value, 
        transform, count_nan, closed_interval,
        chromsizes_filename, zoom_step)
    
