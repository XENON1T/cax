"""Add electron lifetime
"""
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
                  NOTE: if you can't express your correction as a sympy function you can add a custom handling
                  by overriding the 'evaluate' method with whatever you need
    We keep track of the correction versions in the run doc, in processor.correction_versions.
    This is a subsection of processor, so the correction versions used in processing will be stored in the processor's
    metadata.
    """
    key = 'not_set'
    collection_name = 'not_set'
    version = 'not_set'

    def __init__(self):
        self.correction_collection = config.mongo_collection(self.collection_name)
        if self.key == 'not_set':
            raise ValueError("You must set a correction key attribute")
        if self.collection_name == 'not_set':
            raise ValueError("You must set a correction collection_name attribute")
        Task.__init__(self)

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
        classname = self.__class__.__name__
        this_run_version = self.run_doc.get('processor', {}).get('correction_versions', {}).get(classname, 'not_set')
        if this_run_version == self.version:
            # No change was made in the correction, nothing to do for this run.
            return

        # We have to (re)compute the correction setting value. This is done in the evaluate method.
        # There used to be an extra check for self.key: {'$exists': False} in the query, but now that we allow
        # automatic updates of correction values by cax this is no longer appropriate.
        try:
            self.collection.find_one_and_update({'_id': self.run_doc['_id']},
                                                {'$set': {self.key: self.evaluate(),
                                                          'processor.correction_versions.' + classname: self.version}})
        except RuntimeError as e:
            self.log.exception(e)

    def evaluate(self):
        raise NotImplementedError

    def evaluate_function(self, **kwargs):
        """Evaluate the sympy function of this correction with the given kwargs"""
        return self.function.evalf(subs=kwargs)


class AddElectronLifetime(CorrectionBase):
    """Insert the electron lifetime appropriate to each run"""
    key = 'processor.DEFAULT.electron_lifetime_liquid'
    collection_name = 'purity'

    def evaluate(self):
	t = sympy.symbols('t', integer=True)
        lifetime = self.evaluate_function(t=self.run_doc['start'].timestamp())
        lifetime = float(lifetime)      # Convert away from Sympy type.
        self.log.info("Run %d: calculated lifetime of %d us" % (self.run_doc['number'], lifetime))

        return lifetime * units.us


class AddDriftVelocity(CorrectionBase):
    key = 'processor.DEFAULT.drift_velocity_liquid'
    collection_name = 'drift_velocity'

    def evaluate(self):
        run_number = self.run_doc['number']

        # Minimal init of hax. It's ok if hax is inited again with different settings before or after this.
        hax.init(pax_version_policy='loose', main_data_paths=[])

        # Get the cathode voltage in kV
        cathode_kv = hax.slow_control.get('XE1T.GEN_HEINZVMON.PI', run_number).mean()

        # Get the drift velocity
        value = float(self.evaluate_function(v=cathode_kv))

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

	gains = []
	for i in range(0, len(PAX_CONFIG['DEFAULT']['pmts'])):
	    gain = self.function.evalf(subs={pmt: i,
                                             t: self.run_doc['start'].timestamp(),
                                             't0': 0
                                       })
            gains.append(float(gain) * self.correction_units)

        return gains


class SetNeuralNetwork(CorrectionBase):
    '''Set the proper neural network file according to run number'''
    key = "processor.NeuralNet.PosRecNeuralNet.neural_net_file"
    collection_name = 'neural_network'
    
    def evaluate(self):
        number = self.run_doc['number']
        for rdef in self.correction_doc['correction']:
            if number >= rdef['min'] and number < rdef['max']:
                return rdef['value']
        return None

class SetFieldDistortion(CorrectionBase):
	'''Set the proper field distortion map according to run number'''
	key = 'processor.WaveformSimulator.rz_position_distortion_map'
	collection_name = 'field_distortion'

	def evaluate(self):
		number = self.run_doc['number']
		for rdef in self.correction_doc['correction']:
			if number >= rdef['min'] and number < rdef['max']:
				return rdef['value']
		return None

class SetLightCollectionEfficiency(CorrectionBase):
	'''Set the proper LCE map according to run number'''
	key = 'processor.WaveformSimulator.s1_light_yield_map'
	collection_name = 'light_collection_efficiency'

	def evaluate(self):
		number = self.run_doc['number']
		for rdef in self.correction_doc['correction']:
			if number >= rdef['min'] and number < rdef['max']:
				return rdef['value']
		return None

class SetS2xyMap(CorrectionBase):
    """Set the proper S2 x, y map according to run number"""
    key = 'processor.WaveformSimulator.s2_light_yield_map'
    collection_name = 's2_xy_map'
    
    def evaluate(self):
        number = self.run_doc['number']
        for rdef in self.correction_doc['correction']:
            if number >= rdef['min'] and number < rdef['max']:
                return rdef['value']
        return None
