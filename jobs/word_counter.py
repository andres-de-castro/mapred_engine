import re
import sys

from collections import defaultdict

import helpers
from helpers.pre_process import read_file
from helpers.parse_args import args

Base = helpers.parse_args.get_map_reduce_class()

WORD_RE = re.compile(r"[\w']+")

class WordCounter(Base):

    def mapper(self, line):
        for word in WORD_RE.findall(line):
            yield word, 1


    def reducer(self, key, entries):
        return key, sum(entries)


if __name__ == '__main__':
    results = sorted(
                WordCounter().run(),
                key=lambda x: x[1],
                reverse=True
    )
    for result in results:
        print(result[0], result[1])
