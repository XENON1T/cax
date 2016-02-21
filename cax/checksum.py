import config
import checksumdir

def checksums(missing_only = True):
    hostname = config.get_hostname()

    collection = config.mongo_collection()

    for doc in collection.find({'detector' : 'tpc'}):

        for datum in doc['data']:
            if datum['status'] != 'transferred':
               continue

            if 'host' not in datum or datum['host'] != hostname:
                continue

            if 'checksum' in datum and datum['checksum'] != None and missing_only:
                continue

            value = checksumdir.dirhash(datum['location'],
                                        'sha512')

            if datum['checksum'] == None:
                datum['checksum'] = value
            
                print("Updating", doc['name'])
                collection.update({'_id': doc['_id'],
                                   'data.type' : 'raw'},
                                  {'$set': {'data.$' : datum}})
            else:
                assert datum['checksum'] == value

checksums(False)
