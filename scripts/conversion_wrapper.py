import argparse
import os
import sys

from cooler.contrib import recursive_agg_onefile
import tileBedFileByImportance, tile_bigWig

def main():
    parser = argparse.ArgumentParser(
        description="Wrapper around conversion tools available for higlass")
    parser.add_argument(
        '-i', '--input_file', help="Path to input file", required=True)
    parser.add_argument(
        '-o', '--output_file', help="Path of output file", required=False)
    parser.add_argument(
        '-d', '--data_type', choices=[
            "bigwig", "cooler", "gene_annotation", "hitile"],
        help='Data Type of input file.', required=True)

    args = vars(parser.parse_args())

    data_type = args["data_type"]
    input_file = args["input_file"]
    input_file_basename = os.path.basename(input_file)
    output_file = args["output_file"]

    if output_file is None:
        output_file = format_output_filename(input_file_basename, data_type)

    if output_file == "-":
        sys.stdout.write("Output to stdout")

    if data_type in ["bigwig", "hitile"]:
        sys.argv = [input_file, "-o {}".format(output_file)]
        tile_bigWig.main()

    if data_type == "cooler":
        sys.argv = []
        recursive_agg_onefile.main(input_file, output_file)

    if data_type == "gene_annotation":
        sys.argv = [input_file, output_file]
        tileBedFileByImportance.main()


def format_output_filename(input_file_basename, data_type):
    """
        Takes an input_file_basename and data_type and returns the properly
        formatted output filename
        :param input_file_basename: String
        :param data_type: String
    """

    ext = data_type if not data_type == "gene_annotation" else "bed"

    return "{}.{1}.multires.{1}".format(input_file_basename, ext)


if __name__ == '__main__':
    main()