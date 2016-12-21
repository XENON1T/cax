"""Add electron lifetime
"""
import datetime
from collections import defaultdict

import numpy as np
import os
import sympy
import time
import pytz
import requests
from hax import slow_control
from pax import configuration, units
from sympy.parsing.sympy_parser import parse_expr

from cax import config
from cax.task import Task

PAX_CONFIG = configuration.load_configuration('XENON1T')
PAX_CONFIG_MV = configuration.load_configuration('XENON1T_MV')

class CorrectionBase(Task):
    "Derive correction"

    collection_name = 'purity'

    def __init__(self):
        self.correction_collection = config.mongo_collection(self.collection_name)
        Task.__init__(self)

    def evaluate(self):
        raise NotImplementedError()

    def each_run(self):
        if self.key == 'slow_control' and 'slow_control' in self.run_doc:
            return
        else:
            short_key = self.key.split('.')[-1]
            if 'processor' in self.run_doc and \
                  'DEFAULT' in self.run_doc['processor'] and \
                   short_key in self.run_doc['processor']['DEFAULT']:
                return

        if 'end' not in self.run_doc:
            return

        self.get_correction()

        if not config.DATABASE_LOG:
            return

        # Update run database
        try:
            self.collection.find_and_modify({'_id'   : self.run_doc['_id'],
                                             self.key: {'$exists': False}},
                                            {'$set': {self.key: self.evaluate()}})
        except RuntimeError as e:
            self.log.exception(e)

    def get_correction(self):
        # Fetch the latest electron lifetime fit
        doc = self.correction_collection.find_one(sort=(('calculation_time',
                                                         -1),))
        # Get fit function
        self.function = parse_expr(doc['function'])

    def get_time_range(self):
        if self.run_doc['end'] < datetime.datetime(2016, 7, 20):
            dt = datetime.timedelta(minutes=30)
        else:
            dt = datetime.timedelta(minutes=3)

        return (self.run_doc['start'] - dt,
                self.run_doc['end'] + dt)


class AddElectronLifetime(CorrectionBase):
    """Copy data to here

    If data exists at a reachable host but not here, pull it.
    """
    key = 'processor.DEFAULT.electron_lifetime_liquid'
    correction_units = units.us

    def evaluate(self):
        # Compute lifetime from this function on this dataset
        subs = {"t": self.run_doc['start'].timestamp()}
        lifetime = self.function.evalf(subs=subs)
        lifetime = float(lifetime)  # Convert away from Sympy type.

        run_number = self.run_doc['number']
        self.log.info("Run %d: calculated lifetime of %d us" % (run_number,
                                                                lifetime))
        return lifetime * self.correction_units


class AddSlowControlInformation(CorrectionBase):
    """Add all slow control highlight information to run db
    """
    key = 'slow_control'
    correction_units = 1

    def get_correction(self):
        pass

    def evaluate(self):
        time_range = self.get_time_range()

        data = defaultdict(dict)
        for key1, value1 in slow_control.VARIABLES.items():
            for key2, value2 in value1.items():
                series = slow_control.get_series(value2, time_range)
                if len(series):
                    data[key1][key2] = float(series.iloc[0])
                else:
                    data[key1][key2] = np.nan

        return data


class AddGains(CorrectionBase):
    """Copy data to here

    If data exists at a reachable host but not here, pull it.
    """
    collection_name = 'gains'
    key = 'processor.DEFAULT.gains'
    correction_units = units.V  # should be 1

    def each_run(self):
        """Only run on Midway
        """
        if all(x in config.get_hostname() for x in ["midway", "login1"]): 
            CorrectionBase.each_run(self)

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

        # Grab voltages from SC
        self.log.info("Getting voltages at %d" % timestamp)
        voltages = np.array(self.get_voltages(timestamp))

        number_important = len(slow_control.VARIABLES['pmts'])
        if -1 in voltages[0:number_important]:
            missing_pmts = np.where(voltages[0:number_important] == -1)[0]
            names = [slow_control.VARIABLES['pmts']['pmt_%03d_bias_V' % mp]
                     for mp in missing_pmts]
            self.log.error("SCfail %d %d %s" % (self.run_doc['number'],
                                                timestamp,
                                                " ".join(names)))
            raise RuntimeError("Missing SC variable")

        gains = []
        for i, voltage in enumerate(voltages):
            self.log.debug("Deriving HV for PMT %d" % i)
            gain = self.function.evalf(subs={V  : float(voltage),
                                              pmt: i})
            gains.append(float(gain) * self.correction_units)

        return gains

    def get_voltages(self, timestamp):
        try:
            r = requests.post('https://xenon1t-daq.lngs.infn.it/slowcontrol/getLastMeasuredPMTValues',
                          data = {'EndDateUnix' : int(timestamp),
                                  'username':'slowcontrolwebserver',
                                  'api_key' : os.environ.get('api_key'),
                              },
                          verify=False)

            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.exception(e)
            self.log.info("Sleeping 10 seconds, then retrying")
            time.sleep(10)
            return self.get_voltages(timestamp)

        pmts = slow_control.VARIABLES['pmts']
        mapping = {v: int(k.split('_')[1]) for k,v in pmts.items()}

        voltages = len(PAX_CONFIG['DEFAULT']['pmts'])*[-1]

        json_value = r.json()

        if not isinstance(json_value, list):
            raise RuntimeError(str(json_value))

        for doc in json_value:
            if doc['tagname'] in mapping.keys():
                voltages[mapping[doc['tagname']]] = doc['value'] if doc['value'] > 1 else 0

        return voltages

