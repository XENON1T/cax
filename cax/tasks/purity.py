"""Add electron lifetime
"""

from sympy.parsing.sympy_parser import parse_expr
from pax import units

from cax import config
from cax.task import Task


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


        function = parse_expr(doc['electron_lifetime_function'])

        # Compute value from this function on this dataset
        lifetime = function.evalf(subs={"t" : self.run_doc['start'].timestamp()})

        run_number = self.run_doc['number']
        self.log.info("Run %d: calculated lifetime of %d us" % (run_number,
                                                                lifetime))

        if not config.DATABASE_LOG:
            return

        # Update run database
        key = 'processor.DEFAULT.electron_lifetime_liquid'
        self.collection.find_and_modify({'_id': self.run_doc['_id']},
                                        {'$set': {key: lifetime * units.us}})
