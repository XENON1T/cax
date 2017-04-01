# Import the lone_run_collection fixture, which should be given as an argument to any test.
# flake8 will complain it's not used, please ignore this (just loading the fixture does the magic)
from .common import lone_run_collection, runs_db
import pytest
from datetime import datetime

from cax.tasks.corrections import CorrectionBase

class ElectronHappinessCorrection(CorrectionBase):
    key = 'processor.DEFAULT.electron_happiness_liquid'
    collection_name='electron_happiness'

    def evaluate(self):
        return float(self.evaluate_function())


@pytest.fixture()
def correction_collection():
    """Fixture that creates a correction collection with a single entry, then cleans it up
    """
    corr_collection = runs_db['electron_happiness']
    corr_collection.insert_one(dict(
        calculation_time=datetime.now(),
        version='1.0.0',
        comment='Very nice correction',
        function='1',           # Notice it's a string: this is evaluated as a sympy expression.
    ))
    yield corr_collection
    corr_collection.delete_many({})


def test_correction(lone_run_collection, correction_collection):
    t = ElectronHappinessCorrection()
    t.go()

    def get_run_doc():
        return lone_run_collection.find_one({})

    # Check the correction has been inserted
    run_doc = get_run_doc()
    assert 'processor' in run_doc
    assert 'DEFAULT' in run_doc['processor']
    assert 'electron_happiness_liquid' in run_doc['processor']['DEFAULT']
    assert run_doc['processor']['DEFAULT']['electron_happiness_liquid'] == 1

    # Check the version was inserted
    assert 'correction_versions' in run_doc['processor']
    assert 'ElectronHappinessCorrection' in run_doc['processor']['correction_versions']
    assert run_doc['processor']['correction_versions']['ElectronHappinessCorrection'] == '1.0.0'

    # Check that the correction is NOT recalculated when the correction version hasn't changed
    correction_collection.find_one_and_update({},
                                              {'$set': {'function': '2'}})
    t.go()
    run_doc = get_run_doc()
    assert run_doc['processor']['DEFAULT']['electron_happiness_liquid'] == 1

    # Check it IS recalculated when the version changes
    correction_collection.find_one_and_update({},
                                              {'$set': {'function': '3',
                                                        'version': '1.1.0'}})
    t.go()
    run_doc = get_run_doc()
    assert run_doc['processor']['DEFAULT']['electron_happiness_liquid'] == 3

    # Check calculation time will act as version if version is ommitted
    correction_collection.find_one_and_update({},
                                              {'$unset': {'version': ''},
                                               '$set': {'function': '4'}})
    t.go()
    run_doc = get_run_doc()
    assert run_doc['processor']['DEFAULT']['electron_happiness_liquid'] == 4
    calc_time_string = str(correction_collection.find_one()['calculation_time'])
    assert run_doc['processor']['correction_versions']['ElectronHappinessCorrection'] == calc_time_string
