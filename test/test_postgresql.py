#! /usr/bin/env python

'''Make sure the Mongo client is sane'''

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import unittest
from test import BaseTest
from simhash_db import Client


class PostgresTest(BaseTest, unittest.TestCase):
    '''Test the ElasticSearch client'''
    def make_client(self, name, num_blocks, num_bits):

        return Client('postgresql', name, num_blocks, num_bits)

if __name__ == '__main__':
    unittest.main()

