import pytest
from .common import lone_run_collection


def test_task(lone_run_collection):
    """Tests the basic task framework."""
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
    if run_status == 'verifying':
        t.go()
    else:
        # If the status is anything but verifgying, but the checksum is missing, AddChecksum will raise something
        # Currently it's not a great exception (keyerror)
        with pytest.raises(Exception):
            t.go()
