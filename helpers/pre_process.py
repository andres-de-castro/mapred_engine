import yaml
import itertools

def read_file(file_name):
    with open(file_name, 'r') as f:
        yield from f


def read_specific_lines(file_name, line_numbers):
    with open(file_name, 'r') as f:
        for index, line in enumerate(f):
            if index in line_numbers:
                yield line


def yaml_read(file_name):
    lines = read_file(file_name)
    for line in lines:
        yield yaml.safe_load(line)


def yaml_to_map(file_name, key, value):
    records = yaml_read(file_name)
    return {record[key]:record[value] for record in records}


def count_lines(file_name):
    return sum(1 for line in open(file_name))


def chunkify_lines(chunks, l, n_lines):
    ranges = []
    start = 0
    for _ in range(chunks-1):
        old_start = start
        start = start + l
        ranges.append(range(old_start, start))
    ranges.append(range(start, n_lines + 1))
    return ranges


