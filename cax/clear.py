import config
import pymongo
import os


def remove_untriggered(datum):
    client = pymongo.MongoClient(datum['location'])
    db = client.untriggered
    db.authenticate('eb',
                    os.environ.get('MONGO_PASSWORD'))
    print("Dropping", datum['collection'])
    db.drop_collection(datum['collection'])


def check_copies(copies):
    checksums = [x['checksum'] for x in copies]
    for checksum in checksums:
        assert checksum == checksums[0]

def clear():
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # For each TPC run, check if should be uploaded
    for doc in collection.find({'detector' : 'tpc'}):
        mongo_untriggered = None
        copies = []

        if 'data' not in doc:
            continue
        
        # Iterate over data locations to know status
        for datum in doc['data']:
            # If not transfered
            if datum['status'] != 'transferred':  continue

            if datum['type'] == 'untriggered':
                mongo_untriggered = datum
            elif datum['type'] == 'raw':
                copies.append(datum)

        #print('%08d' % doc['number'], mongo_untriggered, len(copies), check_copies(copies))

        if len(copies) > 2:
            collection.update({'_id': doc['_id']},
                              {'$pull' : {'data': mongo_untriggered}})
                               
            remove_untriggered(mongo_untriggered)
#        print(here, copies)



if __name__ == "__main__":
    clear()
