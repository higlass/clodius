import os.path as op
from pydantic import BaseModel

FILETYPES = {
    "bam": {
        "description": "Read mappings",
        "extensions": [".bam"],
        "datatypes": ["reads", "alignments"],
    },
    "chromsizes-tsv": {
        "description": "Chromosome sizes",
        "extensions": [".chromsizes", ".fai", ".chrom.sizes"],
        "datatypes": ["chromsizes"],
    },
    "cooler": {
        "description": "multi-resolution cooler file",
        "extensions": [".mcool"],
        "datatypes": ["matrix"],
    },
    "bigwig": {
        "description": "Genomics focused multi-resolution vector file",
        "extensions": [".bw", ".bigwig"],
        "datatypes": ["vector"],
    },
    "bedfile": {
        "description": "BED file",
        "extensions": [".bed", ".bed.gz", ".bed.bgz"],
        "datatypes": ["bedlike", "gene-annotations"],
    },
    "beddb": {
        "description": "SQLite-based multi-resolution annotation file",
        "extensions": [".beddb", ".multires.db"],
        "datatypes": ["bedlike", "gene-annotations"],
    },
    "fasta": {
        "description": "FASTA sequence file",
        "extensions": [".fa", ".fna", ".fasta"],
        "datatypes": ["sequence"],
    },
    "gff": {
        "description": "General feature format",
        "extensions": [".gff", ".gff.gz", ".gff.bgz"],
        "datatypes": ["bedlike"],
    },
    "hitile": {
        "description": "Multi-resolution vector file",
        "extensions": [".hitile"],
        "datatypes": ["vector"],
    },
    "multivec": {
        "description": "Multi-sample vector file",
        "extensions": [".multivec"],
        "datatypes": ["multivec"],
    },
    "time-interval-json": {
        "description": "Time interval notation",
        "extensions": [".htime"],
        "datatypes": ["time-interval"],
    },
}


def infer_filetype(filename):
    for filetype, meta in FILETYPES.items():
        for ext in meta["extensions"]:
            if filename.endswith(ext.lower()):
                return filetype

    return None


def infer_datatype(filetype):
    if filetype in FILETYPES:
        return FILETYPES[filetype]["datatypes"][0]

    return None


def get_file_compression(f) -> str:
    """Get the compression type for an open file pointer.

    Can recognize 'gz', 'bz2', 'zip' or 'xz' from the magic number.

    :param f: The file pointer
    :returns: The compression type."""
    magic_dict = {
        b"\x1f\x8b\x08": "gzip",
        b"\x42\x5a\x68": "bz2",
        b"\x50\x4b\x03\x04": "zip",
        b"\xfd\x37\x7a\x58\x5a\x00": "xz",
    }

    max_len = max(len(x) for x in magic_dict)

    prev_pos = f.tell()
    file_start = f.read(max_len)
    f.seek(prev_pos)

    for magic, filetype in magic_dict.items():
        # print("l", len(file_start), "file_start", file_start)
        if file_start.startswith(magic):
            return filetype

    return None


TILE_OPTIONS_CHAR = ","
