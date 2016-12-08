import os
import tempfile

import mongomock
import pytest

from cax import config as cax_config

# Check if we could possibly access the real runs db; if so abort the tests.
if os.environ.get('MONGO_PASSWORD') is not None:
    raise RuntimeError("You have the MONGO_PASSWORD environment variable set. "
                       "It is not safe to run the cax tests with the possibility of accessing the true runs db.")

# Replace cax's mongo_collection with a mongomock collection
mongo_url = 'mongodb://eb:None@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
runs_collection = mongomock.MongoClient(mongo_url)['run']['runs_new']
cax_config.mongo_collection = lambda name='runs_new': runs_collection

# Pretend to be midway
cax_config.HOST = 'midway-login1'


@pytest.fixture(params=['transferred', 'verifying'])
def lone_run_collection(request):
    with tempfile.TemporaryDirectory() as dirname:

        # Create some content standing in for the data
        with open(os.path.join(dirname, 'example_data.txt'), mode='w') as outfile:
            outfile.write("Hi there cax tester!")

        runs_collection.insert_one({'number': 1,
                                    'data': [
                                        {'host': 'midway-login1',
                                         'location': os.path.abspath(dirname),
                                         'status': request.param,
                                         'type': 'raw'}
                                    ]})

        yield runs_collection

    runs_collection.delete_many({})
