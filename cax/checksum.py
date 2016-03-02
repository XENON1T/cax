from . import config
import checksumdir
import hashlib
import logging

from cliff.command import Command

def filehash(location):
    sha = hashlib.sha512()
    with open(location, 'rb') as f:
        while True:
            block = f.read(2**10) # Magic number: one-megabyte blocks.
            if not block: break
            sha.update(block)
    return sha.hexdigest()

class Checksum(Command):
    "Perform a checksum on accessible data."

    log = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Checksum, self).get_parser(prog_name)
        parser.add_argument('repeat', action='store_true')
        return parser

    def take_action(self, parsed_args):
        collection = config.mongo_collection()

        for doc in collection.find({'detector' : 'tpc'}):
            if 'data' not in doc:
                continue

            for datum in doc['data']:
                if datum['status'] != 'verifying':
                    continue

                if datum['type'] != 'raw':
                    continue

                if 'host' not in datum or datum['host'] != config.get_hostname():
                    continue

                if 'checksum' in datum and datum['checksum'] != None and not parsed_args.repeat:
                    continue

                value = checksumdir.dirhash(datum['location'],
                                            'sha512')

                if datum['checksum'] == None:
                    datum['checksum'] = value
                    datum['status'] = 'transferred'

                    print("Updating", doc['name'])
                    collection.update({'_id': doc['_id'],
                                       'data.host' : datum['host']},
                                      {'$set': {'data.$' : datum}})
                else:
                    assert datum['checksum'] == value

