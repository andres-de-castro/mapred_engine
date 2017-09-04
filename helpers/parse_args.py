import argparse
from mapred_engine.map_reduce import MapReduceSingleCore, MapReduceMultiCore

parser = argparse.ArgumentParser(
        description=
                    """
                    Run MapReduce jobs
                    """
        )

parser.add_argument('args', nargs='*')

parser.add_argument(
                    '-m',
                    required=False,
                    type=bool,
                    default=False,
                    help='Specify running jobs in single/mp mode'
)

args = vars(parser.parse_args())

def get_map_reduce_class():
    if args['m']:
        return MapReduceMultiCore
    else:
        return MapReduceSingleCore
