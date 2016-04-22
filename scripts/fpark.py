import collections as col
import itertools as it

class ParallelData:
    def __init__(self, data):
        self.data = data

    def map(self, func):
        return ParallelData(map(func, self.data))

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
