import pymongo
import os
import sys
import datetime
from pax import __version__

def make_runlist():
    uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
    uri = uri % os.environ.get('MONGO_PASSWORD')
    c = pymongo.MongoClient(uri,
                            replicaSet='runs',
                            readPreference='secondaryPreferred')
    db = c['run']
    collection = db['runs_new']
    
    query = {"$or" : [{"detector" : "tpc",
                       "$and" : [{"number" : {"$gt" : 15000}}] 
                       #"$and" : [{"number" : {"$gt" : 6731}},
                                 #{"number" : {"$lt" : 11000}} # to specify range of run numbers
                       },
                      {"detector" : "muon_veto",  # UNCOMMENT TO INCLUDE MV AFTER DATETIME BELOW
                       #"end" : {"$gt" : (datetime.datetime(2017, 7, 29, 00, 00, 00))} # ALE 
                       "end" : {"$gt" : (datetime.datetime(2017, 12, 1, 00, 00, 00))}} # Feb 1 2017 at midnight
                      # }
                      ], 
                      
#             'tags.name' : '_sciencerun1', # if you want to specify a tag             

#             'source.type' : 'none',  # if you want to specify a source

             'reader.ini.write_mode' : 2,
             'trigger.events_built' : {"$gt" : 0},
             'processor.DEFAULT.gains' : {'$exists' : True},
             'processor.DEFAULT.drift_velocity_liquid' : {'$exists' : True},
             'processor.correction_versions': {'$exists': True},
#             'processor.WaveformSimulator': {'$exists': True},
             'processor.NeuralNet|PosRecNeuralNet': {'$exists': True},
#             '$and' : [{'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess'}}}},
#                       {'tags' : {"$not" : {'$elemMatch' : {'name':'messy'}}}},
#                       {'tags' : {"$not" : {'$elemMatch' : {'name':'test'}}}},
#                       {'tags' : {"$not" : {'$elemMatch' :{'name':'bad'}}}},
#                       {'tags' : {'$elemMatch' : {'name' : '_sciencerun0_candidate'}}}
#                       ]
#             'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess'}}},
#             'tags' : {"$not" : {'$elemMatch' : {'name' : 'test'}}},
#             'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess', 'name':'messy', 'name':'test','name':'bad'}}},
#             'tags' : {'$elemMatch' : {'name' : '_sciencerun0_candidate'}},
             }

    version = 'v' + __version__

    cursor = collection.find(query, {"number" : True,
                                     "name" : True,
                                     "data" : True,
                                     "detector" : True,
                                     "_id" : False})
    
    cursor = list(cursor)
    
    print("Total runs: %d" % len(cursor))
    bad = []
    processed_list = []
    processing = []
    error = []
    can_process = []
    cant_process = []
    
    for run in cursor:
        on_rucio = False
        processed = False
        on_stash = False
        on_midway = False


        if 'data' not in run:
            bad.append(run[_id])
            continue

        if run['detector'] == 'tpc':
            _id = 'number'
        else:
            _id = 'name'

        for d in run['data']:
            if d['type'] == 'processed' and 'pax_version' in d:
                if d['pax_version'] == version and d['status'] == 'transferred':
                    processed  = True
                    #continue

                elif d['pax_version'] == version and d['status'] == 'transferring' and d['host'] == 'login':
                    processing.append(run[_id])
                    #continue
                
                elif d['pax_version'] == version and d['status'] == 'error' and d['host'] == 'login':
                    error.append(run[_id])

            if d['host'] == 'rucio-catalogue' and d['type']=='raw' and d['status'] == 'transferred':
                on_rucio = True

            elif d['host'] == 'login' and d['type']=='raw' and d['status'] == 'transferred':
                if os.path.exists(d['location']):
                    on_stash = True

            elif d['host'] == 'midway-login1' and d['type']=='raw' and d['status'] == 'transferred':
                on_midway = True

        if processed:
            processed_list.append(run[_id])
            continue
        if run[_id] in processing:
            continue

        if (on_rucio or on_stash or on_midway):
            can_process.append(run[_id])
            #print(run[_id])
        else:
            cant_process.append(run[_id])
                
                

    #return stashlist
    print("BAD: %d" % len(bad))
    print("PROCESSED ALREADY: %d" % len(processed_list))
    print("PROCESSING NOW: %d" % len(processing))
    print("ERROR: %d" % len(error))
    print("CAN PROCESS: %d" % len(can_process))
    print("CANNOT PROCESS: %d" % len(cant_process))
    print(cant_process)

    return can_process

if __name__ == '__main__':
    make_runlist()

