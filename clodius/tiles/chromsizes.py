import csv
import logging
from smart_open import open

logger = logging.getLogger(__name__)


def tileset_info(filename):
    chromsizes = get_tsv_chromsizes(filename)

    max_width = sum([int(c[1]) for c in chromsizes])
    return {
        "max_width": max_width,
        "chromsizes": [[c[0], int(c[1])] for c in chromsizes],
        "min_pos": [0],
        "max_pos": [max_width],
    }


def get_tsv_chromsizes(file):
    """
    Get a list of chromosome sizes from this [presumably] tsv
    chromsizes file file.

    Parameters:
    -----------
    file: string
        A file-like object

    Returns
    -------
    chromsizes: [(name:string, size:int), ...]
        An ordered list of chromosome names and sizes
    """
    if isinstance(file, str):
        file = open(file, "rb")

    try:
        file.seek(0)
        binary_data = file.read()
        text_data = binary_data.decode("utf-8")

        lines = text_data.split("\n")
        data = [l.strip().split("\t") for l in lines if l.strip()]
        return data
    except Exception as ex:
        logger.error(ex)

        err_msg = "WHAT?! Could not load file %s." % (ex)

        raise Exception(err_msg)
