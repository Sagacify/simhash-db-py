
import struct
from . import BaseClient


def unsigned_to_signed(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!q', struct.pack('!Q', integer))[0]


def signed_to_unsigned(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!Q', struct.pack('!q', integer))[0]


class SimHashHelper(BaseClient):
    '''Our ES backend client'''
    def __init__(self, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, 'noneed', num_blocks, num_bits)

    def build_simhash_indexes(self, hsh):
        return dict((
                str(i),
                unsigned_to_signed(int(self.corpus.tables[i].permute(hsh)))
            ) for i in range(self.num_tables))

    def get_simhash(self, hashOn):
        '''
        Add simhash properties to a document
        Hashing from the field "hashOn" and putting the simhash
        structure in the field "hashField"
        '''

        hsh = simhash.hash(hashOn)

        # Construct the simhash indexes
        hshStruct = self.build_simhash_indexes(hsh)

        # Insert simhash indexes
        doc[hashField] = hshStruct

        return doc
