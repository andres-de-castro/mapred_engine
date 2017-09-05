import sys

from collections import defaultdict

class MapReduceBase(object):

    def __init__(self):
        self.file_name = sys.argv[1]

    def mapper(self):
        raise NotImplementedError


    def reducer(self):
        raise NotImplementedError


    def reduce_combine(self, acc):
        return [self.reducer(key, acc[key]) for key in acc]


    def accumulate(self, iterable):
        acc = defaultdict(list)
        for line in iterable:
            if self.line_parse:
                line = self.line_parse(line)
            mapped = self.mapper(line)
            for record in mapped:
                acc[record[0]].append(record[1])
        return acc
