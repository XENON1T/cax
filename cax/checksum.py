import config
import checksumdir

def checksums(missing_only = True):
    collection = config.mongo_collection()

    for doc in collection.find({'detector' : 'tpc'}):

        for datum in doc['data']:
            if datum['status'] != 'verifying':
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
