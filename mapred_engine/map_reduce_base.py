class MapReduceBase(object):

    def combiner(self):
        raise NotImplementedError


    def mapper(self):
        raise NotImplementedError


    def reducer(self):
        raise NotImplementedError