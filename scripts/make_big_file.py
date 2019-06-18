#!/usr/bin/python

import argparse
import random


def main():
    """
    python make_big_file.py length

    Create a big file with one column and 'length' rows.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("length", type=int)
    parser.add_argument("-l", "--limit", default=100, type=int)

    args = parser.parse_args()

    for i in range(args.length):
        print(random.randint(0, args.limit))

    # entries.reduceByKey(reduceByKeyFunc


if __name__ == "__main__":
    main()
