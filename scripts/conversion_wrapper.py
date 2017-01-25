import argparse
import os
import sys

def set_postmortem_hook():
    import sys, traceback, ipdb
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
        description="Wrapper around conversion tools available for higlass")
    parser.add_argument(
        '-i', '--input_file', help="Path to input file", required=True)
    parser.add_argument(
        '-o', '--output_file', help="Path of output file", required=False)
    parser.add_argument(
        '-d', '--data_type', choices=[
            "bigwig", "cooler", "gene_annotation", "hitile"],
        help='Data Type of input file.', required=True)
    parser.add_argument(
        '-n', '--n_cpus',
        help='Number of cpus to use for converting cooler files',
        required=False, default=1, type=int)
    parser.add_argument(
        '-c', '--chunk-size',
        help='Number of records each worker handles at a time',
        required=False, default=int(1e6), type=int)

    args = vars(parser.parse_args())

    data_type = args["data_type"]
    input_file = args["input_file"]
    output_file = args["output_file"]
    n_cpus = args["n_cpus"]
    chunk_size = args["chunk_size"]


    if output_file is None:
        output_file = format_output_filename(input_file, data_type)

    if output_file == "-":
        sys.stdout.write("Output to stdout")

    if data_type in ["bigwig", "hitile"]:
        sys.argv = ["fake.py", input_file, "-o", output_file]
        import  tile_bigWig

        tile_bigWig.main()

    if data_type == "cooler":
        from cooler.contrib import recursive_agg_onefile
        recursive_agg_onefile.main(
            input_file, output_file, chunk_size, n_cpus=n_cpus)

    if data_type == "gene_annotation":
        sys.argv = ["fake.py", input_file, output_file]
        import tileBedFileByImportance
        tileBedFileByImportance.main()


def format_output_filename(input_file, data_type):
    """
    Takes an input_file and data_type and returns the properly
    formatted output filename
    :param input_file: String
    :param data_type: String
    """

    input_file_basename = os.path.basename(input_file)

    file_extentions = {
        "gene_annotation" : "bed",
        "hitile" : "hitile",
        "cooler" : "cool",
        "bigwig": "bw"
    }

    return "{}.multires.{}".format(
        input_file_basename.rpartition(".")[0], file_extentions[data_type])


if __name__ == '__main__':
    main()
