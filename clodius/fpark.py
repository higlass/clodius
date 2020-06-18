import collections as col
import functools as ft
import glob
import itertools as it
import os.path as op


class ParallelData:
    def __init__(self, data):
        self.data = data

    def map(self, func):
        return ParallelData([func(d) for d in self.data])

    def count(self):
        return len(self.data)

    def union(self, other_data):
        self.data = self.data + other_data.data
        return self

    def getNumPartitions(self):
        return 1

    def countByKey(self):
        """
        Count the number of elements with a particular key.
        """
        counts = col.defaultdict(int)

        for d in self.data:
            counts[d[0]] += 1

        return counts

    def coalesce(self, num):
        return self

    def groupByKey(self):
        """
        When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
        """
        buckets = col.defaultdict(list)

        for d in self.data:
            buckets[d[0]].append(d[1])

        return ParallelData(buckets.items())

    def flatMap(self, func):
        """
        Flatten a list of results mapped to the data
        and return as one large list.
        """
        result = map(func, self.data)

        return ParallelData(list(it.chain.from_iterable(result)))

    def foreach(self, func):
        for d in self.data:
            func(d)

    def foreachPartition(self, func):
        func(self.data)

    def reduceByKey(self, func):
        """
        When called on a dataset of (K, V) pairs, returns a dataset of (K, V)
        pairs where the values for each key are aggregated using the given
        reduce function func, which must be of type (V,V) => V. Like in
        groupByKey, the number of reduce tasks is configurable through an
        optional second argument.
        """
        buckets = col.defaultdict(list)
        for d in self.data:
            buckets[d[0]].append(d[1])

        reduced_buckets = dict()
        for key in buckets:
            reduced_buckets[key] = ft.reduce(func, buckets[key])

        return ParallelData(reduced_buckets.items())

    def aggregateByKey(self, start_val, seq_func, comb_func):
        buckets = col.defaultdict(list)
        for d in self.data:
            buckets[d[0]].append(d[1])

        reduced_buckets = dict()
        for key in buckets:
            comb_val = start_val.copy()
            for val in buckets[key]:
                comb_val = seq_func(comb_val, val)
            reduced_buckets[key] = comb_val

        return ParallelData(reduced_buckets.items())

    def reduce(self, func):
        return ft.reduce(func, self.data)

    def collect(self):
        return self.data

    def take(self, n):
        return self.data[:n]


class FakeSparkContext:
    """
    Emulate a SparkContext for local processing.
    """

    def __init__(self):
        pass

    @staticmethod
    def parallelize(data):
        return ParallelData(data)

    @staticmethod
    def singleTextFile(filename):
        """
        Load a single file as a ParallelData set.

        :param filename: A file to be loaded line by line
        @return: A ParallelData object wrapping the lines in the file.
        """
        with open(filename, "r") as f:
            return ParallelData(list(map(lambda x: x.strip(), f.readlines())))

    @staticmethod
    def textFile(filename):
        """
        Load a filename as a text file. Filename can be either a single file
        or a directory containing a multitude of 'part-*' files.

        :param filename: The name of the file (or directory) containing the data which
                         we want to load one by one
        :return: A ParallelData object containing all of the lines of the file or files
        """
        if op.isdir(filename):
            parts_files = glob.glob(op.join(filename, "part-*"))
            p = FakeSparkContext.parallelize([])

            for filename in parts_files:
                p = p.union(FakeSparkContext.singleTextFile(filename))
            return p
        else:
            return FakeSparkContext.singleTextFile(filename)
