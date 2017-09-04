import re
import sys

from collections import defaultdict

import helpers
from helpers.pre_process import yaml_read, yaml_to_map
from helpers.parse_args import args

import yaml

MapReduce = helpers.parse_args.get_map_reduce_class()

class AverageRatings(MapReduce):

    def __init__(self):
        #let's overwrite the file reader with something better
        super(AverageRatings, self).__init__(
                file_reader=yaml_read,
                line_parse=yaml.safe_load
        )


    def mapper(self, line):
        #filter user_id
        filters =  ['movie_id', 'rating']
        filtered = dict((key,value) for key, value in line.items() if key in filters)
        yield filtered['movie_id'], filtered['rating']


    def reducer(self, key, entries):
        return key, (sum(entries) / len(entries))


if __name__ == '__main__':
    semantic_map = yaml_to_map('data/raw/movies.txt', key='id', value='name')
    results = AverageRatings().run()
    for result in results:
        print(semantic_map[result[0]], result[1])
