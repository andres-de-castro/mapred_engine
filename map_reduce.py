import sys

from helpers.pre_process import read_file
from collections import defaultdict

class MapReduce(object):

    def __init__(self, file_reader=read_file):
        self.file_name = sys.argv[1]
        self.file_reader = file_reader


    def combiner(self):
        raise NotImplementedError


    def mapper(self):
        raise NotImplementedError


    def reducer(self):
        raise NotImplementedError


    def execute(self):
        acc = defaultdict(list)
        lines = self.file_reader(self.file_name)
        results = []
        for line in lines:
            mapped = self.mapper(line)
            while True:
                try:
                    entry = next(mapped)
                except StopIteration:
                    break
                combined = self.combiner(entry)
                acc[combined[0]].append(combined[1])
        for key in acc:
            reduced = self.reducer(key, acc[key])
            results.append(reduced)
        return results


    def run(self):
        results = self.execute()
        return results
