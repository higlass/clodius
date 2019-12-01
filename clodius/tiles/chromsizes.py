import csv
import logging

logger = logging.getLogger(__name__)


def get_tsv_chromsizes(filename):
    """
    Get a list of chromosome sizes from this [presumably] tsv
    chromsizes file file.

    Parameters:
    -----------
    filename: string
        The filename of the tsv file

    Returns
    -------
    chromsizes: [(name:string, size:int), ...]
        An ordered list of chromosome names and sizes
    """
    try:
        with open(filename, "r") as f:
            reader = csv.reader(f, delimiter="\t")

            data = []
            for row in reader:
                data.append(row)
        return data
    except Exception as ex:
        logger.error(ex)

        err_msg = "WHAT?! Could not load file %s. 😤 (%s)" % (filename, ex)

        raise Exception(err_msg)
