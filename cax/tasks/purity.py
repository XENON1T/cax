"""Add electron lifetime
"""

import dill as pickle
from pax import units

from cax import config
from ..task import Task

class AddElectronLifetime(Task):
    "Add electron lifetime to dataset"

    def __init__(self):
        self.collection_purity = config.mongo_collection('purity')
        Task.__init__(self)

    def each_run(self):
        if 'processor' in self.run_doc:
            return

        # Fetch the latest electron lifetime fit
        doc = self.collection_purity.find_one(sort=(('calculation_time',
                                                     -1),))

        # This following import is actually used when evaluating the lifetime function
        # noinspection PyUnresolvedReferences
        import numpy as np

        # Arguments from the fit (required to execute function below even
        # though it doesn't appear to be called.
        popt = pickle.loads(doc['popt'])

        # Fit function
        f = pickle.loads(doc['electron_lifetime_function'])

        # Compute value from this function on this dataset
        lifetime = f(self.run_doc['start'].timestamp())

        self.log.info("Calculated lifetime of %d us" % lifetime)

        if not config.DATABASE_LOG:
            return

        # Update run database
        key = 'processor.DEFAULT.electron_lifetime_liquid'
        self.collection.find_and_modify({'_id': self.run_doc['_id']},
                                        {'$set': {key: lifetime * units.us}})
