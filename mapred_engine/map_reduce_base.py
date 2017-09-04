from collections import defaultdict
from itertools import chain

class MapReduceBase(object):


    def combiner(self):
        raise NotImplementedError


    def mapper(self):
        raise NotImplementedError


    def reducer(self):
        raise NotImplementedError


    def reduce_combine(self, acc):
        return [self.reducer(key, acc[key]) for key in acc]
