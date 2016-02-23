import config
import checksumdir
import hashlib

def filehash(location):
    sha = hashlib.sha512()
    with open(location, 'rb') as f:
        while True:
            block = f.read(2**10) # Magic number: one-megabyte blocks.
            if not block: break
            sha.update(block)
    return sha.hexdigest()

def checksums(missing_only = True):
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

            if 'checksum' in datum and datum['checksum'] != None and missing_only:
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

checksums()
