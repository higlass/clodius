import collections as col
import itertools as it

class ParallelData:
    def __init__(self, data):
        self.data = data

    def map(self, func):
        return ParallelData(map(func, self.data))

    def count(self):
        return len(self.data)

    def union(self, other_data):
        self.data = self.data + other_data.data
        return self

    def countByKey(self):
        '''
        Count the number of elements with a particular key.
        '''
        counts = col.defaultdict(int)

        for d in self.data:
            counts[d[0]] += 1

        return counts

    def groupByKey(self):
        '''
        When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. 
        '''
        buckets = col.defaultdict(list)

        for d in self.data:
            buckets[d[0]].append(d[1])

        return ParallelData(buckets.items())

    def flatMap(self, func):
        '''
        Flatten a list of results mapped to the data
        and return as one large list.
        '''
        result = map(func, self.data)

        return ParallelData(list(it.chain.from_iterable(result)))

    def foreach(self, func):
        map(func, self.data)

    def reduceByKey(self, func):
        '''

        When called on a dataset of (K, V) pairs, returns a dataset of (K, V)
        pairs where the values for each key are aggregated using the given
        reduce function func, which must be of type (V,V) => V. Like in
        groupByKey, the number of reduce tasks is configurable through an
        optional second argument.
        
        '''
        buckets = col.defaultdict(list)
        for d in self.data:
            buckets[d[0]].append(d[1])

        reduced_buckets = dict()
        for key in buckets:
            reduced_buckets[key] = reduce(func, buckets[key])

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
        return reduce(func, self.data)

    def collect(self):
        return self.data

class FakeSparkContext:
    '''
    Emulate a SparkContext for local processing.
    '''
    def __init__(self):
        pass

    @staticmethod
    def parallelize(data):
        return ParallelData(data)
