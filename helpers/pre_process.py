import yaml
import itertools


def read_file(file_name):
    with open(file_name, 'r') as f:
        for line in f:
            yield line


def yaml_read(file_name):
    lines = read_file(file_name)
    lines = [yaml.safe_load(line) for line in lines]
    return lines


def yaml_to_map(file_name, key, value):
    records = yaml_read(file_name)
    return {record[key]:record[value] for record in records}


def chunkify(iterable, n_chunks):
    chunks = tuple(list() for _ in range(n_chunks))
    cycle_range = itertools.cycle(range(n_chunks))
    for item in iterable:
        chunks[next(cycle_range)].append(item)

    return chunks
