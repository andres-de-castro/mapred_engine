import sys
import multiprocessing

from helpers.pre_process import read_file, chunkify
from collections import defaultdict

from mapred_engine.map_reduce_base import MapReduceBase


class MapReduceSingleCore(MapReduceBase):

    def __init__(self, file_reader=read_file):
        self.file_name = sys.argv[1]
        self.file_reader = file_reader


    def execute(self):
        lines = self.file_reader(self.file_name)
        acc = self.map_combine(lines)
        results = self.reduce_combine(acc)

        return results


    def run(self):
        results = self.execute()
        return results


class MapReduceMultiCore(MapReduceBase):

    def __init__(self, file_reader=read_file):
        self.file_name = sys.argv[1]
        self.file_reader = file_reader
        self.cores = multiprocessing.cpu_count()


    def split_input(self):
        lines = self.file_reader(self.file_name)
        chunks = chunkify(lines, self.cores)
        return chunks


    def execute(self, chunk):
        acc = self.map_combine(chunk)
        results = self.reduce_combine(acc)
        return results


    def join_reduce(self, results):
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
        joined = self.join_reduce(results)
        return joined
