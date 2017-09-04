import yaml
import itertools

def read_file(file_name):
    with open(file_name, 'r') as f:
        yield from f


def read_chunks_from_file(file_name, n_lines):
    with open(file_name) as f:
        for next_n_lines in itertools.zip_longest(*[f] * n_lines):
            yield next_n_lines


def yaml_read(file_name):
    lines = read_file(file_name)
    for line in lines:
        yield yaml.safe_load(line)


def yaml_to_map(file_name, key, value):
    records = yaml_read(file_name)
    return {record[key]:record[value] for record in records}


def count_lines(file_name):
    count = 0
    for line in read_file(file_name):
        count += 1
    return count


#thanks nedbar
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def chunkify(iterable, n_chunks):
    chunks = tuple(list() for _ in range(n_chunks))
    cycle_range = itertools.cycle(range(n_chunks))
    for item in iterable:
        chunks[next(cycle_range)].append(item)

    return chunks


def chunkify_lines(chunks, l, n_lines):
    ranges = []
    start = 1
    for _ in range(chunks-1):
        old_start = start
        start = start + l
        ranges.append(range(old_start, start))
    ranges.append(range(start, n_lines + 1))
    return ranges


