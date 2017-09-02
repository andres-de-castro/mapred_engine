import sys
import multiprocessing

from helpers.pre_process import read_file, chunkify
from collections import defaultdict
from functools import partial

class MapReduce(object):

    def __init__(self, file_reader=read_file):
        self.file_name = sys.argv[1]
        self.file_reader = file_reader
        self.cores = multiprocessing.cpu_count()


    def combiner(self):
        raise NotImplementedError


    def mapper(self):
        raise NotImplementedError


    def reducer(self):
        raise NotImplementedError


    def split_input(self):
        lines = self.file_reader(self.file_name)
        chunks = chunkify(lines, self.cores)
        return chunks


    def execute(self, chunk):
        acc = defaultdict(list)
        for line in chunk:
            # print(line)
            mapped = self.mapper(line)
            while True:
                try:
                    entry = next(mapped)
                except StopIteration:
                    break
                combined = self.combiner(entry)
                acc[combined[0]].append(combined[1])
        acc_results = []
        for key in acc:
            reduced = self.reducer(key, acc[key])
            acc_results.append(reduced)
        return acc_results


    def join_results(self, results):
        acc = defaultdict(list)
        for result in results:
            for record in result:
                acc[record[0]].append(record[1])
        acc_results = []
        for key in acc:
            reduced = self.reducer(key, acc[key])
            acc_results.append(reduced)
        return acc_results


    def run(self):
        chunks = self.split_input()
        with multiprocessing.Pool(processes=self.cores) as pool:
            results = pool.map_async(self.execute, chunks).get()
        pool.join()
        joined = self.join_results(results)
        return joined
