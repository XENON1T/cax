"""Add electron lifetime
"""

import pickle

import numpy as np
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

        # Arguments from the fit (required to execute function below even
        # though it doesn't appear to be called.
        popt = pickle.loads(doc['popt'])

        # Fit function
        if doc['electron_lifetime_function'] == 'exponential':
            f = lambda x: np.exp((x - popt[0].copy()) / popt[1].copy())
        else:
            raise NotImplementedError()

        # Compute value from this function on this dataset
        lifetime = f(self.run_doc['start'].timestamp())

        self.log.info("Calculated lifetime of %d us" % lifetime)

        if not config.DATABASE_LOG:
            return

        # Update run database
        key = 'processor.DEFAULT.electron_lifetime_liquid'
        self.collection.find_and_modify({'_id': self.run_doc['_id']},
                                        {'$set': {key: lifetime * units.us}})
