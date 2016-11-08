#! /usr/bin/env python

'''This is a utility to run a benchmark against a backend'''

import os
import time
import random
import argparse
from simhash_db import Client, GeneralException

parser = argparse.ArgumentParser(description='Run benchmarks on simhash_db')
parser.add_argument(
    '--count', type=int, default=1,
    help='How many thousands of keys should be inserted per process')
parser.add_argument(
    '--name', type=str, default='testing',
    help='The name of the set of simhashes to use')
parser.add_argument(
    '--processes', type=int, default=1,
    help='How many processes should be forked')
parser.add_argument(
    '--num-blocks', dest='num_blocks', type=int, default=6,
    help='How many blocks to configure the client to use')
parser.add_argument(
    '--num-bits', dest='num_bits', type=int, default=3,
    help='How many bits to configure the client to use')
parser.add_argument(
    '--backend', type=str, required=True,
    help='Which backend to use')
parser.add_argument(
    '--config', type=str, required=False,
    help='Path to a yaml file with the host configuration')

args = parser.parse_args()

insert_per_process = 10000
query_per_process = 1000

# If a configuration file was provided, we should use it
if args.config:
    from yaml import load
    with open(args.config) as fin:
        kwargs = load(fin.read())
else:
    kwargs = {}


def make_seeds(number):
    '''
    Generate all the hashes that we'll be using. We'd like to be able to get a
    large number of hashes for insertion, but we also don't want to:

      1) use a lot of memory holding said set
      2) spend a lot of time generating that set

    So, we generate a number of random seed values, and then insert 1000 hashes
    beginning with that number and skipping by another random number. These
    pairs of (seed, skip) are returned'''
    return [(
        random.randint(0, 2 ** 64),
        random.randint(1, number)
    ) for i in range(number)]

def insert():
    '''Run the timing numbers for each of the provided seeds for insertion'''
    seeds = make_seeds(insert_per_process)
    client = Client(
        args.backend, args.name, args.num_blocks, args.num_bits, **kwargs)
    # for i in range(insert_per_process):
    #     if i % (insert_per_process/10) == 0:
    #         print('Inserting batch %i' % i)
    #     # We want to get a relatively good mix each time we insert data
    hashes = [(start + interval) for start, interval in seeds]
    try:
        results = client.insert(hashes)
    except GeneralException as exc:
        print('---> Client exception: %s' % repr(exc))

    # Since this is meant to be run in a subprocess...
    print("Done inserting")
    exit(0)


def query():
    '''Run the timing numbers for each of the provided seeds for query all'''
    print("Start quering")
    seeds = make_seeds(query_per_process)
    client = Client(
        args.backend, args.name, args.num_blocks, args.num_bits, **kwargs)

    hashes = [(start + interval) for start, interval in seeds]
    for hash in hashes:
        # if i % (query_per_process/10) == 0:
        #     print('Querying batch %i' % i)
        # We want to get a relatively good mix each time we insert data
        # hashes = [(start + i * interval) for start, interval in seeds]
        try:
            results = client.find_all(hash)
        except GeneralException as exc:
            print('---> Client exception: %s' % repr(exc))

    # Since this is meant to be run in a subprocess...
    print("Done querying")
    exit(0)


def time_children(processes, function, *args, **kwargs):
    '''Run `processes` number of forked processes, each running `function` with
    the provided arguments, and return the timing information'''
    start = -time.time()
    times = []
    for i in range(processes):
        pid = os.fork()
        if pid == 0:
            function(*args, **kwargs)
        else:
            print('---> Started %i' % pid)

    # And now wait for them, and collect the timing information
    for i in range(processes):
        pid, status = os.wait()
        times.append(start + time.time())
        print('---> %i finished in %fs' % (pid, times[-1]))

    return times


def clear_database():
    client = Client(
        args.backend, args.name, args.num_blocks, args.num_bits, **kwargs)
    client.delete()

# Now run the benchmark itself and print out the results
insert_times = time_children(args.processes, insert)
query_times = time_children(args.processes, query)

# This is how many hashes we actually tried to insert
insert_count = insert_per_process * args.count
query_count = query_per_process * args.count

print('Insertion:')
print('    Total insertions: %d' % insert_count)
print('    Times (min | avg | max): %10.5f s   | %10.5f s   | %10.5f s  ' % (
    min(insert_times), sum(insert_times) / len(insert_times),
    max(insert_times)))
print('    Rate  (min | avg | max): %10.5f / s | %10.5f / s | %10.5f / s' % (
    insert_count / max(insert_times), insert_count * len(insert_times) / sum(insert_times),
    insert_count / min(insert_times)))

print('Query:')
print('    Total queries: %d' % query_count)
print('    Times (min | avg | max): %10.5f s   | %10.5f s   | %10.5f s  ' % (
    min(query_times), sum(query_times) / len(query_times), max(query_times)))
print('    Rate  (min | avg | max): %10.5f / s | %10.5f / s | %10.5f / s' % (
    query_count / max(query_times), query_count * len(query_times) / sum(query_times),
    query_count / min(query_times)))

# Delete database after statistics collection
clear_database()
