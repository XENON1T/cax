"""Add electron lifetime
"""
import sympy
import datetime
from hax import slow_control
from pax import configuration, units
from sympy.parsing.sympy_parser import parse_expr

from cax import config
from cax.task import Task

PAX_CONFIG = configuration.load_configuration('XENON1T')


class CorrectionBase(Task):
    "Derive correction"

    def __init__(self):
        self.correction_collection = config.mongo_collection(
            self.collection_name)
        Task.__init__(self)

    def evaluate(self):
        raise NotImplementedError()

    def each_run(self):
        short_key = self.key.split('.')[-1]
        if 'processor' in self.run_doc and \
                        'DEFAULT' in self.run_doc['processor'] and \
                        short_key in self.run_doc['processor']['DEFAULT']:
            return

        if 'end' not in self.run_doc:
            return

        # Fetch the latest electron lifetime fit
        doc = self.correction_collection.find_one(sort=(('calculation_time',
                                                         -1),))

        print(doc.keys(), doc)
        # Get fit function
        self.function = parse_expr(doc['function'])

        if not config.DATABASE_LOG:
            return

        # Update run database
        self.collection.find_and_modify({'_id'   : self.run_doc['_id'],
                                         self.key: {'$exists': False}},
                                        {'$set': {self.key: self.evaluate()}})


class AddElectronLifetime(CorrectionBase):
    """Copy data to here

    If data exists at a reachable host but not here, pull it.
    """
    collection_name = 'purity'
    key = 'processor.DEFAULT.electron_lifetime_liquid'
    correction_units = units.us

    def evaluate(self):
        # Compute lifetime from this function on this dataset
        lifetime = self.function.evalf(
            subs={"t": self.run_doc['start'].timestamp()})
        lifetime = float(lifetime)  # Convert away from Sympy type.

        run_number = self.run_doc['number']
        self.log.info("Run %d: calculated lifetime of %d us" % (run_number,
                                                                lifetime))
        return lifetime * self.correction_units


class AddGains(CorrectionBase):
    """Copy data to here

    If data exists at a reachable host but not here, pull it.
    """
    collection_name = 'gains'
    key = 'processor.DEFAULT.gains'
    correction_units = units.V  # should be 1

    n_channels = 248

    def evaluate(self):
        """Make an array of all PMT gains."""
        n_channels = len(PAX_CONFIG['DEFAULT']['pmts'])
        gains = [float(self.get_gain(i)) for i in range(n_channels)]
        self.log.info("Run %d: gains of:" % self.run_doc['number'])
        self.log.info(gains)
        return gains

    def get_gain(self, pmt_location):
        """Grab a derived gain.

        pmt_location is the PMT number.  t0 and t1 are datetime objects.

        The variables fed in can be used for making a gain decision.
        """
        self.log.debug("Grabbing HV for PMT %d" % pmt_location)

        if self.run_doc['end'] < datetime.datetime(2016, 7, 20):
            dt = datetime.timedelta(minutes=30)
        else:
            dt = datetime.timedelta(minutes=3)

        time_range = (self.run_doc['start'] - dt,
                      self.run_doc['end'] + dt)

        voltages = None

        # Name of the slow-control variable
        key = 'pmt_%03d_bias_V' % pmt_location
        if key in slow_control.VARIABLES['pmts']:
            name = slow_control.VARIABLES['pmts'][key]

            # Fetch from slow control
            voltages = slow_control.get_series(name,
                                               time_range=time_range)

        # If no values found, use default gain.
        if voltages is None or voltages.count() == 0:
            return float(2e6)

        self.log.debug("Deriving HV for PMT %d" % pmt_location)
        V = sympy.symbols('V')
        pmt = sympy.symbols('pmt', integer=True)
        result = self.function.evalf(subs={V  : voltages.median(),
                                           pmt: pmt_location})
        return float(result) * self.correction_units
