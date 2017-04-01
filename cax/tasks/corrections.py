"""Add electron lifetime
"""
import datetime

import numpy as np
import sympy
import pytz
import hax
from pax import configuration, units
from sympy.parsing.sympy_parser import parse_expr

from cax import config
from cax.task import Task

PAX_CONFIG = configuration.load_configuration('XENON1T')
PAX_CONFIG_MV = configuration.load_configuration('XENON1T_MV')

class CorrectionBase(Task):
    """Base class for corrections.

    Child classes can set the following class attributes:
        key:  run doc setting this correction will override (usually processor.XXX,
                                                             you can leave off processor if you want)
        collection_name: collection in the runs db where the correction values are stored.

    We expect documents in the correction contain:
      - calculation_time: timestamp, indicating when YOU made this correction setting
    And optionally also:
      - version: string indicating correction version. If not given, timestamp-based version will be used.
      - function: sympy expression, passed to sympy.evaluate then stored in the 'function' instance attribute

    We keep track of the correction versions in the run doc, in processor.correction_versions.
    This is a subsection of processor, so the correction versions used in processing will be stored in the processor's
    metadata.
    """
    key = 'correction.setting.not.given'
    collection_name = 'purity'
    version = 'not_set'

    def __init__(self):
        self.correction_collection = config.mongo_collection(self.collection_name)
        Task.__init__(self)

    def evaluate(self):
        raise NotImplementedError()

    def each_run(self):
        if 'end' not in self.run_doc:
            # Run is still in progress, don't compute the correction
            return

        if not config.DATABASE_LOG:
            # This setting apparently means we should do nothing?
            return

        # Fetch the latest correction settings.
        # We can't do this in init: cax is a long-running application, and corrections may change while it is running.
        self.correction_doc = cdoc = self.correction_collection.find_one(sort=(('calculation_time', -1), ))
        self.version = cdoc.get('version', str(cdoc['calculation_time']))

        # Get the correction sympy function, if one is set
        if 'function' in cdoc:
            self.function = parse_expr(cdoc['function'])

        # Check if this correction's version correction has already been applied. If so, skip this run.
        this_run_version = self.run_doc.get('processor', {}).get('correction_versions', {}).get(self.__class__.__name__,
                                                                                                'not_set')
        if this_run_version == self.version:
            # No change was made in the correction, nothing to do for this run.
            return

        # We have to recompute the correction. This is done in the evaluate method.
        # There used to be an extra check for self.key: {'$exists': False} in the query, but now that we allow updates
        # this is no longer appropriate.
        try:
            self.collection.find_and_modify({'_id': self.run_doc['_id']},
                                            {'$set': {self.key: self.evaluate()}})
        except RuntimeError as e:
            self.log.exception(e)

    def evaluate_function(self, **kwargs):
        """Evaluate the sympy function of this correction with the given kwargs"""
        return self.function.evalf(subs=kwargs)


class AddElectronLifetime(CorrectionBase):
    """Insert the electron lifetime appropriate to each run"""
    key = 'processor.DEFAULT.electron_lifetime_liquid'

    def evaluate(self):
        lifetime = self.evaluate_function(t=self.run_doc['start'].timestamp())
        lifetime = float(lifetime)      # Convert away from Sympy type.
        self.log.info("Run %d: calculated lifetime of %d us" % (self.run_doc['number'], lifetime))

        return lifetime * units.us


class AddDriftVelocity(CorrectionBase):
    key = 'processor.DEFAULT.drift_velocity_liquid'
    collection = 'drift_velocity'

    def evaluate(self):
        run_number = self.run_doc['number']

        # Minimal init of hax. It's ok if hax is inited again with different settings before or after this.
        hax.init(pax_version_policy='loose', main_data_paths=[])

        # Get the cathode voltage in kV
        cathode_kv = hax.slow_control.get('XE1T.GEN_HEINZVMON.PI', run_number).mean()

        # Get the drift velocity
        value = self.evaluate_function(v=cathode_kv)

        self.log.info("Run %d: calculated drift velocity of %0.3f km/sec" % (run_number, value))
        return value * units.km / units.s


class AddGains(CorrectionBase):
    """Add PMT gains to each run"""
    key = 'processor.DEFAULT.gains'
    collection_name = 'gains'
    correction_units = units.V  # should be 1

    def evaluate(self):
        """Make an array of all PMT gains."""
        start = self.run_doc['start']
        timestamp = start.replace(tzinfo=pytz.utc).timestamp()

        if self.run_doc['reader']['self_trigger']:
            self.log.info("Run %d: gains computing" % self.run_doc['number'])
            gains = self.get_gains(timestamp)
        elif self.run_doc['detector'] == 'muon_veto':
            self.log.info("Run %d: using 1e6 as gain for MV" % self.run_doc['number'])
            gains = len(PAX_CONFIG_MV['DEFAULT']['pmts'])*[1e6]
        else:
            self.log.info("Run %d: using 1 as gain for LED" % self.run_doc['number'])
            gains = len(PAX_CONFIG['DEFAULT']['pmts'])*[1]

        return gains

    def get_gains(self, timestamp):
        """Timestamp is a UNIX timestamp in UTC
        """
        V = sympy.symbols('V')
        pmt = sympy.symbols('pmt', integer=True)
        t = sympy.symbols('t')

        # Minimal init of hax. It's ok if hax is inited again with different settings before or after this.
        hax.init(pax_version_policy='loose', main_data_paths=[])

        # Grab voltages from SC
        self.log.info("Getting voltages at %d" % timestamp)
        voltages = hax.slow_control.get(['PMT %03d' % x for x in range(254)],
                                         self.run_doc['number']).median().values

        # Append zeros to voltage list, to accomodate acquisition monitor channels.
        voltages.resize(len(PAX_CONFIG['DEFAULT']['pmts']))

        gains = []
        for i, voltage in enumerate(voltages):
            self.log.debug("Deriving HV for PMT %d" % i)
            gain = self.function.evalf(subs={V: float(voltage),
                                             pmt: i,
                                             t: self.run_doc['start'].timestamp(),
                                             't0': 0
                                            })
            gains.append(float(gain) * self.correction_units)

        return gains
