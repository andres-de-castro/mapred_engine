import re
import sys

from collections import defaultdict

from helpers.pre_process import read_file, yaml_read, yaml_to_map
from map_reduce import MapReduce


class AverageRatings(MapReduce):

    def __init__(self):
        super(AverageRatings, self).__init__(file_reader=yaml_read)


    def mapper(self, line):
        yield [(k, v) for k, v in line.items() if k in ['movie_id', 'rating']]


    def combiner(self, entry, key='movie_id', value='rating'):
        named_entry = dict(entry)
        return named_entry[key], named_entry[value]


    def reducer(self, key, entries):
        return key, (sum(entries) / len(entries))


if __name__ == '__main__':

    semantic_map = yaml_to_map('data/raw/movies.txt', key='id', value='name')

    results = AverageRatings().run()

    for result in results:
        print(semantic_map[result[0]], result[1])
