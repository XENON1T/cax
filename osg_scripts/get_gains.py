import pymongo
import os
import sys


def get_runs_with_gains():
    uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
    uri = uri % os.environ.get('MONGO_PASSWORD')
    c = pymongo.MongoClient(uri,
                            replicaSet='runs',
                            readPreference='secondaryPreferred')
    db = c['run']
    collection = db['runs_new']
    
    query = {"detector" : "tpc", 
             "tags" : {"$elemMatch" : {"name" : "_sciencerun0_candidate"}}}
             #"$and" : [{"number" : {"$gt" : 3934}}, {"number" : {"$lt": 6379}}]}
    
    cursor = collection.find(query, {"number" : True,
                                     "processor.DEFAULT" : True,
                                     "_id" : False})
    
    cursor = list(cursor)
    
    total_runs = len(cursor)
    missing_gains = []
    have_gains = []
    for run in cursor:
        if "processor" not in run:
            continue
        if "gains" not in run["processor"]["DEFAULT"]:
            missing_gains.append(run["number"])
        else:
            have_gains.append(run["number"])

    return have_gains            
