import pytest

# Import the lone_run_collection fixture, which should be given as an argument to any task.
# flake8 will complain it's not used, please ignore this (just loading the fixture does the magic)
from .common import lone_run_collection


def test_task(lone_run_collection):
    """Tests the task framework by a simple task that looks at the fake runs db.
    """
    from cax.task import Task

    class ScanningTask(Task):
        data_entries_found = 0

        def each_location(self, data_doc):
            self.data_entries_found += 1
            pass

    t = ScanningTask()
    t.go()
    assert t.data_entries_found == 1


def test_checksum(lone_run_collection):
    run_status = lone_run_collection.find_one({})['data'][0]['status']

    from cax.tasks.checksum import AddChecksum
    t = AddChecksum()

    # There is no checksum in the starting runs collection. This is ok only if we're in verifying or error.
    if run_status in ['verifying', 'error']:
        t.go()
    else:
        with pytest.raises(Exception):
            t.go()
