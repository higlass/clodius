import negspy.coordinates as nc

def load_chromsizes(chromsizes_filename, assembly=None):
    '''
    Load a set of chromosomes from a file or using an assembly
    identifier. If using just an assembly identifier the chromsizes
    will be loaded from the negspy repository.

    Parameters:
    -----------
    chromsizes_filename: string
        The file containing the tab-delimited chromosome sizes
    assembly: string
        Assembly name (e.g. 'hg19'). Not necessary if a chromsizes_filename is passed in
    '''
    if chromsizes_filename is not None:
        chrom_info = nc.get_chrominfo_from_file(chromsizes_filename)
        chrom_names = chrom_info.chrom_order
        chrom_sizes = [chrom_info.chrom_lengths[c] for c in chrom_info.chrom_order]
    else:
        chrom_info = nc.get_chrominfo(assembly)
        chrom_names = nc.get_chromorder(assembly)
        chrom_sizes = nc.get_chromsizes(assembly)

    return (chrom_info, chrom_names, chrom_sizes)
