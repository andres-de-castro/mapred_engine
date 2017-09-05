# mapred_engine

This is an implementation of single / multi-core mapreduce.

The multicore implementation calculates the length of the file, generates a of range objects which represent file line indices and passes them to the subprocess.
Lines are then read via linecache, this allows


Steps:

1) git checkout this repo

2) install venv with:

    >> python3 -m venv env

3) activate the environment

    >> source env/bin/activate

4) run setup.py

    >> pip install -e .

5) Run the scripts, note the -m flag for multicore (default False)

    >> python word_counter.py data/raw/if-kipling.txt -m True

    >> python average_ratings.py data/raw/ratings.txt -m True

