import sys
import multiprocessing
import yaml

from helpers.pre_process import (
    read_file,
    chunkify_lines,
    count_lines,
    read_specific_lines
    )
from collections import defaultdict

from mapred_engine.map_reduce_base import MapReduceBase


class MapReduceSingleCore(MapReduceBase):

    def __init__(self, file_reader=read_file, line_parse=None):
        self.file_name = sys.argv[1]
        self.file_reader = file_reader
        self.line_parse = line_parse


    def execute(self):
        acc = defaultdict(list)
        lines = self.file_reader(self.file_name)

        for line in lines:
            mapped = self.mapper(line)
            for record in mapped:
                acc[record[0]].append(record[1])

        results = self.reduce_combine(acc)
        return results


    def run(self):
        results = self.execute()
        return results


class MapReduceMultiCore(MapReduceBase):

    def __init__(self, file_reader=read_file, line_parse=None):
        self.file_name = sys.argv[1]
        self.file_reader = file_reader
        self.line_parse = line_parse
        self.cores = multiprocessing.cpu_count()


    def split_input(self):
        lines = self.file_reader(self.file_name)
        chunks = chunkify(lines, self.cores)
        yield chunks


    def execute(self, line_numbers):
        acc = defaultdict(list)
        lines = read_specific_lines(self.file_name, line_numbers)
        for line in lines:
            if self.line_parse:
                line = self.line_parse(line)
            mapped = self.mapper(line)
            for record in mapped:
                acc[record[0]].append(record[1])
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
        n_lines = count_lines(self.file_name)
        lines_per_chunk = round(n_lines / self.cores)
        line_numbers = chunkify_lines(self.cores, lines_per_chunk, n_lines)
        with multiprocessing.Pool(processes=self.cores) as pool:
            results = pool.map_async(self.execute, line_numbers).get()
        pool.join()
        joined = self.join_reduce(results)
        return joined
