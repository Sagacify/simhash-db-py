#! /usr/bin/env python

'''Make sure the Mongo client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client
from simhash_db.elasticsearch import SimHashHelper


class ElasticsearchTest(BaseTest, unittest.TestCase):
    '''Test the ElasticSearch client'''
    def make_client(self, name, num_blocks, num_bits):

        return Client('es', name, num_blocks, num_bits,
                      hosts=['elasticsearch'])


if __name__ == '__main__':
    unittest.main()
