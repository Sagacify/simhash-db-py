#! /usr/bin/env python

'''Make sure the Mongo client is sane'''

import unittest
import simhash
from simhash_db.elasticsearch import SimHashHelper


class ElasticsearchTest(unittest.TestCase):

    def test_SimHashHelper(self):
        s = '''To access a queue that belongs to another AWS account, use the
        QueueOwnerAWSAccountId parameter to specify the account ID of the
        queue's owner. The queue's owner must grant you permission to access
        the queue. For more information about shared queue access, see
        AddPermission or go to Shared Queues in
        the Amazon SQS Developer Guide.'''

        simhash = SimHashHelper(num_blocks=6, num_bits=3)
        simhashIndexes = simhash.get_simhash(s)
        print(simhashIndexes)
        self.assertTrue(type(simhashIndexes) is dict)
        self.assertTrue('1' in simhashIndexes)


if __name__ == '__main__':
    unittest.main()
