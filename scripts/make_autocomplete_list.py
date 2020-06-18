#!/usr/bin/python

import clodius.fpark as cfp
import clodius.save_tiles as cst
import argparse


def make_autocomplete_list(entries, options, tile_saver):
    """
    Make a list of autocomplete suggestions for a list of json objects

    :param entries: A list of json objects
    :param options: A set of options (i.e. output_dir, etc...) indicating how
                    the tiles should be created
    :return: A list of tiles
    """

    def entry_to_substrs(entry):
        substrs = {}

        if options.name not in entry:
            # if an entry doesn't have a name field, print a warning and continue
            # print >>sys.stderr, "Found entry without a name:", entry['uid']
            return []
        # for each entry get each substring and add the entry to the list
        # of entries containing that substring
        # these lists will then be pruned down to create autocomplete suggestions
        for size in range(0, len(entry[options.name]) + 1):
            for i in range(len(entry[options.name]) - size + 1):
                substr = entry[options.name][i : i + size]

                # make the substrings file and token friendly
                substr = substr.replace("/", " ").lower()
                substr = " ".join(substr.split()).replace(" ", "_")

                # substrs += [((substr), [entry])]
                substrs[substr] = [entry]

        return substrs.items()

    def reduce_substrs(substrs1, substrs2):
        if options.reverse_importance:
            return sorted(
                substrs1 + substrs2, key=lambda x: -float(x[options.importance])
            )[: options.max_entries_per_autocomplete]
        else:
            return sorted(
                substrs1 + substrs2, key=lambda x: float(x[options.importance])
            )[: options.max_entries_per_autocomplete]

    substr_entries = entries.flatMap(entry_to_substrs)
    print("substr_entries:", substr_entries.take(2))

    reduced_substr_entries = substr_entries.reduceByKey(reduce_substrs)

    def save_substr_entry(entry):
        (substr_key, substr_value) = entry
        tile_saver.save_value("ac_" + substr_key, {"suggestions": substr_value})
        """
        ess.index(es_index,
                  es_doctype,
                  body = {"suggestions": substr_value},
                  id = substr_key)
        """

    reduced_substr_entries.foreach(save_substr_entry)
    tile_saver.flush()


def main():
    parser = argparse.ArgumentParser(
        description="""

    python make_autocomplete_list.py processed-ski-area-elevations.json

    Create jsons for searching for ski areas. These will consist
    of all the n-grams found in the ski area names.
"""
    )

    parser.add_argument("input_file", nargs=1)
    parser.add_argument(
        "-n",
        "--name",
        default="name",
        help="The field in the json entry which specifies its name",
    )
    parser.add_argument(
        "-i",
        "--importance",
        default="importance",
        help="The field in the json entry which specifies how important \
                  it is (more important entries are displayed higher up in \
                  the autocomplete suggestions",
    )
    parser.add_argument(
        "-m",
        "--max-entries-per-autocomplete",
        default=10,
        help="The maximum number of entries to be displayed in the \
                  autocomplete suggestions",
    )
    parser.add_argument(
        "-r",
        "--reverse-importance",
        default=False,
        action="store_true",
        help="Use the reverse sorting of the importance value to gauge \
                  the worth of individual entries",
    )
    parser.add_argument(
        "-c",
        "--column-names",
        help="The column names for the input tsv file",
        default=None,
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        default=None,
        help="The delimiter separating columns in the gene_count file",
    )

    parser.add_argument(
        "--elasticsearch-url",
        help="Specify elasticsearch nodes to push the completions to",
        default=None,
    )
    parser.add_argument("--print-status", action="store_true")

    args = parser.parse_args()

    tile_saver = cst.ElasticSearchTileSaver(
        es_path=args.elasticsearch_url, print_status=args.print_status
    )

    dataFile = cfp.FakeSparkContext.textFile(args.input_file[0])

    if args.column_names is not None:
        args.column_names = args.column_names.split(",")

    if args.delimiter is None:
        dataFile = dataFile.map(lambda x: x.split()).map(
            lambda x: dict(zip(args.column_names, x))
        )
    else:
        print("delimiter:", args.delimiter)
        dataFile = dataFile.map(lambda x: x.split(args.delimiter)).map(
            lambda x: dict(zip(args.column_names, x))
        )

    print("one:", dataFile.take(1))

    make_autocomplete_list(dataFile, args, tile_saver)


if __name__ == "__main__":
    main()
