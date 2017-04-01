"""Common test setup for cax

This will setup a fake runs db and make cax think we're running on midway.
"""
from datetime import datetime
import os
import tempfile

import mongomock
import pytest

from cax import config as cax_config

# Check if we could possibly access the real runs db; if so abort the tests.
# TODO: Probably this is a bit too extreme, and it may mean we can't easily run tests on midway.
if os.environ.get('MONGO_PASSWORD') is not None:
    raise RuntimeError("You have the MONGO_PASSWORD environment variable set. "
                       "It is not safe to run the cax tests with the possibility of accessing the true runs db.")

# Replace cax's mongo_collection with a mongomock collection
runs_collection = mongomock.MongoClient(cax_config.RUNDB_URI)['run']['runs_new']
cax_config.mongo_collection = lambda name='runs_new': runs_collection

# Pretend to be midway
cax_config.HOST = 'midway-login1'


@pytest.fixture(params=['transferred', 'verifying', 'error'])
def lone_run_collection(request):
    """Fixture that sets the fake runs db to have a single run (numbered 1).
    The run is deleted from the fake runs db when the test running the fixture exits.

    Using the fixture params, we ensure all tests are run several times for different run transfer statuses.
    """
    with tempfile.TemporaryDirectory() as dirname:

        # Create some content standing in for the data
        with open(os.path.join(dirname, 'example_data.txt'), mode='w') as outfile:
            outfile.write("Hi there cax tester!")

        runs_collection.insert_one({'number': 1,
                                    'start': datetime.now(),
                                    'end': datetime.now(),          # cax checks this to see a run has ended
                                    'data': [
                                        {'host': 'midway-login1',
                                         'location': os.path.abspath(dirname),
                                         'status': request.param,
                                         'type': 'raw'}
                                    ]})

        yield runs_collection

    runs_collection.delete_many({})
