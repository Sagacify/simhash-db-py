#! /usr/bin/env python

'''Code to connect to the ElasticSearch backend'''

import struct
import time
from elasticsearch import Elasticsearch
from . import BaseClient

from sqlalchemy import create_engine, Column, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, Integer, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Index, MetaData, Table
from sqlalchemy.orm import mapper
from sqlalchemy import or_, and_

from datetime import datetime

def unsigned_to_signed(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!q', struct.pack('!Q', integer))[0]


def signed_to_unsigned(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!Q', struct.pack('!q', integer))[0]

'''SQL Schema declaration with Indexes'''
Base = declarative_base()


class HashPartial(Base):
    __tablename__ = 'hash_partial'
    url = Column(Integer, primary_key=True, autoincrement=True)
    hsh_num = Column(Integer, index=True)
    hsh_perm = Column(BigInteger, index=True)
    time = Column(DateTime, index=True)
    hash = Column(JSONB)

def create_doc_hash(hashJson):
    insertions = list()
    for idx in hashJson.keys():
        insertions.append(
            HashPartial(
                hsh_num=idx,
                hsh_perm=hashJson[idx],
                time=datetime.now(),
                hash=hashJson
            )
        )
    return insertions


def save_doc_hash(session, inserts):
    for i in inserts:
        session.add(i)
    session.commit()


class Client(BaseClient):
    '''Our ES backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)

        self.engine = create_engine(
            "postgresql+psycopg2://postgres@postgresql/postgres").connect()

        self.Base = Base
        self.Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def delete(self):
        '''Delete this database of simhashes'''
        self.session.close()
        self.Base.metadata.drop_all(self.engine)

    def close(self):
        self.session.commit()
        self.session.close()

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
            save_doc_hash(self.session, create_doc_hash(doc))

    def get_find_in_table_query(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        low = unsigned_to_signed(ranges[table_num][0])
        high = unsigned_to_signed(ranges[table_num][1])

        return and_(
            HashPartial.hsh_num == table_num,
            HashPartial.hsh_perm >= low,
            HashPartial.hsh_perm <= high
        )

    def find_in_all_table(self, hsh, ranges):
        # Build ES query
        table_queries = []
        for i in range(self.num_tables):
            table_queries.append(self.get_find_in_table_query(hsh, i, ranges))

        documents = self.session.query(HashPartial).filter(
            or_(q for q in table_queries)).distinct(HashPartial.url).all()

        results = self.parse_result(documents)

        return self.filter_result(results, hsh)

    def parse_result(self, results):
        if len(results) > 0:
            return [d.hash for d in results]
        return []

    def filter_result(self, initResults, hsh, table_num=None):
        '''Filter result to only keep the ones close enough'''
        if table_num is not None:
            table_nums = [table_num]
        else:
            table_nums = range(self.num_tables)

        results = []
        for table_num in table_nums:
            results.extend([
                self.corpus.tables[table_num].unpermute(
                    signed_to_unsigned(int(d[str(table_num)])))
                for d in initResults])

        return [h for h in results if
                self.corpus.distance(h, hsh) <= self.num_bits]

    def find_in_table(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        table_query = self.get_find_in_table_query(hsh, table_num, ranges)

        documents = self.session.query(HashPartial).filter(table_query).all()
        results = self.parse_result(documents)

        return self.filter_result(results, hsh)

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
            if len(results) > 0:
                return results[0]
        return results
