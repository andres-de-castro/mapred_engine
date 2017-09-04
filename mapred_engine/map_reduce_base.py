from collections import defaultdict

class MapReduceBase(object):

    def combiner(self):
        raise NotImplementedError


    def mapper(self):
        raise NotImplementedError


    def reducer(self):
        raise NotImplementedError


    def map_combine(self, chunk):
        acc = defaultdict(list)
        for line in chunk:
            mapped = self.mapper(line)
            while True:
                try:
                    record = next(mapped)
                except StopIteration:
                    break
                acc[record[0]].append(record[1])
        return acc


    def reduce_combine(self, acc):
        return [self.reducer(key, acc[key]) for key in acc]