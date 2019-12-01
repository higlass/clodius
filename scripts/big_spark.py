#!/usr/bin/python

import argparse
from pyspark import SparkContext


def main():
    """
    python big_spark.py

    Parse a large input file, group its input by key and output it somewhere.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("input_file")

    args = parser.parse_args()

    sc = SparkContext(appName="big-spark-test")
    entries = sc.textFile(args.input_file).map(lambda x: x.strip().split(" "))

    print("entries.take(1):", entries.take(1))

    # entries.reduceByKey(reduceByKeyFunc


if __name__ == "__main__":
    main()
