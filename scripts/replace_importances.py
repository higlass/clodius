#!/usr/bin/python

import argparse


def main():
    parser = argparse.ArgumentParser(
        description="""

    python replace_importances.py dest_gene_pred source_gene_pred
"""
    )

    parser.add_argument("dest")
    parser.add_argument("source")
    # parser.add_argument('-o', '--options', default='yo',
    # help="Some option", type='str')
    # parser.add_argument('-u', '--useless', action='store_true',
    # help='Another useless option')

    args = parser.parse_args()

    old_importances = {}

    with open(args.source, "r") as f:
        for line in f:
            parts = line.split("\t")
            old_importances[parts[3]] = int(parts[4])

    with open(args.dest, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            new_importance = 0

            if parts[3] in old_importances:
                new_importance = old_importances[parts[3]]

            parts[4] = str(new_importance)

            print("\t".join(parts))


if __name__ == "__main__":
    main()
