#! /usr/bin/env python

'''Code to connect to the ElasticSearch backend'''

import struct
import time
from elasticsearch import Elasticsearch
from . import BaseClient


def unsigned_to_signed(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!q', struct.pack('!Q', integer))[0]


def signed_to_unsigned(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!Q', struct.pack('!q', integer))[0]


def get_es_connection(*args, **kwargs):
    time.sleep(5)
    try:
        print('Establishing connection...')
        client = Elasticsearch(*args, **kwargs)
        client.cluster.health(wait_for_status='yellow', request_timeout=10)
        return client
    except Exception:
        print('Connection failed... Trying again.')
        time.sleep(1)
        return get_es_connection(*args, **kwargs)


class Client(BaseClient):
    '''Our ES backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)

        self.client = get_es_connection(*args, **kwargs)

        self.namePrefix = name + '-'

        self.name = name

        self.client.indices.create(index=self.name, ignore=400)

    def delete(self):
        '''Delete this database of simhashes'''
        self.client.indices.delete(index=self.name, ignore=[400, 404])

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        # Construct the docs, and then we'll do an insert
        docs = [
            dict((
                str(i),
                unsigned_to_signed(int(self.corpus.tables[i].permute(hsh)))
            ) for i in range(self.num_tables)) for hsh in hashes
        ]

        for doc in docs:
            self.client.index(index=self.name, doc_type=self.name, body=doc)

        # Force index to refresh
        self.client.indices.refresh(index=self.name)

    def get_find_in_table_query(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        low = unsigned_to_signed(ranges[table_num][0])
        high = unsigned_to_signed(ranges[table_num][1])

        return {
            "query": {
                "range": {
                    str(table_num): {
                        "gte": low,
                        "lte": high
                    }
                }
            }
        }

    def find_in_all_table(self, hsh, ranges):
        # Build ES query
        table_queries = []
        for i in range(self.num_tables):
            es_table_query = self.get_find_in_table_query(hsh, i, ranges)
            table_queries.append(es_table_query['query'])
        esQuery = {
            "query": {
                "bool": {
                    "should": table_queries
                }
            }
        }

        esRes = self.client.search(index=self.name, body=esQuery)

        results = self.parse_es_result(esRes)
        return self.filter_result(results, hsh)

    def parse_es_result(self, esResults):
        if esResults is not None and esResults['hits']['total'] > 0:
            return [d['_source'] for d in esResults['hits']['hits']]
        return []

    def filter_result(self, initResults, hsh, table_num=None):
        '''Filter result to only keep the ones close enough'''
        if table_num is not None:
            table_nums = [table_num]
        else:
            table_nums = range(self.num_tables)

        results = []
        for table_num in table_nums:
            results.extend([self.corpus.tables[table_num].unpermute(
                signed_to_unsigned(int(d[str(table_num)]))) for d in initResults])

        return [h for h in results if
                self.corpus.distance(h, hsh) <= self.num_bits]

    def find_in_table(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        esQuery = self.get_find_in_table_query(hsh, table_num, ranges)
        try:
            esRes = self.client.search(index=self.name, body=esQuery)
        except Exception:
            esRes = None
        results = self.parse_es_result(esRes)

        return self.filter_result(results, hsh, table_num)

    def find_one(self, hash_or_hashes):
        '''Find one near-duplicate for the provided query (or queries)'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        results = []

        for hsh in hashes:
            ranges = self.ranges(hsh)
            found = []
            for i in range(self.num_tables):
                found = self.find_in_table(hsh, i, ranges)
                if found:
                    results.append(found[0])
                    break
            if found:
                break

        if not found:
            results.append(None)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results

    def find_all(self, hash_or_hashes):
        '''Find all near-duplicates for the provided query (or queries)'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        results = []
        for hsh in hashes:
            ranges = self.ranges(hsh)
            found = self.find_in_all_table(hsh, ranges)
            if found:
                results.append(found)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results
