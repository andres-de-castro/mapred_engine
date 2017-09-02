import re
import sys

from collections import defaultdict

from helpers.pre_process import read_file
from map_reduce import MapReduce

WORD_RE = re.compile(r"[\w']+")

class WordCounter(MapReduce):


    def mapper(self, line):
        for word in WORD_RE.findall(line):
            yield word, 1


    def combiner(self, entry):
        return entry


    def reducer(self, key, entries):
        return key, sum(entries)


if __name__ == '__main__':
    results = WordCounter().run()
    for result in results:
        print(result[0], result[1])
