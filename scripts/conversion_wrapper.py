from __future__ import print_function

import argparse
import clodius.cli.aggregate as cca
import os.path as op
import sys


def set_postmortem_hook():
    import sys
    import traceback
    import ipdb

    def _excepthook(exc_type, value, tb):
        traceback.print_exception(exc_type, value, tb)
        print()
        ipdb.pm()

    sys.excepthook = _excepthook


set_postmortem_hook()


# the appropriate aggregation modules will be imported in their time and
# place


def main():

    parser = argparse.ArgumentParser(
        description="Wrapper around conversion tools available for higlass"
    )
    parser.add_argument("-i", "--input-file", help="Path to input file", required=True)
    parser.add_argument(
        "-o", "--output-file", help="Path of output file", required=False
    )
    parser.add_argument(
        "-d",
        "--filetype",
        choices=["bigwig", "cooler", "gene_annotation", "hitile"],
        help="Data Type of input file.",
        required=True,
    )
    parser.add_argument(
        "-a",
        "--assembly",
        choices=["hg19", "mm9"],
        help="Genome assembly to use",
        required=False,
        default="hg19",
    )
    parser.add_argument(
        "-n",
        "--n-cpus",
        help="Number of cpus to use for converting cooler files",
        required=False,
        default="1",
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        help="Number of records each worker handles at a time",
        required=False,
        default=str(int(10e6)),
    )

    args = vars(parser.parse_args())

    assembly = args["assembly"]
    filetype = args["filetype"]
    input_file = args["input_file"]
    output_file = args["output_file"]
    n_cpus = args["n_cpus"]
    chunk_size = args["chunk_size"]

    if output_file is None:
        output_file = format_output_filename(input_file, filetype)

    print("Output file:", output_file, file=sys.stderr)
    if output_file == "-":
        sys.stdout.write("Output to stdout")

    if filetype in ["bigwig", "hitile"]:
        cca._bigwig(filepath=input_file, output_file=output_file, assembly=assembly)

    if filetype == "cooler":
        from cooler.contrib import recursive_agg_onefile

        sys.argv = [
            "fake.py",
            input_file,
            "-o",
            output_file,
            "-c",
            chunk_size,
            "-n",
            n_cpus,
        ]
        recursive_agg_onefile.main()

    if filetype == "gene_annotation":
        sys.argv = ["fake.py", input_file, output_file, "-a", assembly]
        import tileBedFileByImportance

        tileBedFileByImportance.main()


def format_output_filename(input_file, filetype):
    """
    Takes an input_file and filetype and returns the properly
    formatted output filename
    :param input_file: String
    :param filetype: String
    """
    file_extentions = {
        "gene_annotation": "bed",
        "hitile": "hitile",
        "cooler": "cool",
        "bigwig": "bw",
    }

    return "{}.multires.{}".format(
        op.splitext(input_file)[0], file_extentions[filetype]
    )


if __name__ == "__main__":
    main()
