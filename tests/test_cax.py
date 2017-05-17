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

    def data_doc():
        return lone_run_collection.find_one({})['data'][0]

    run_status = data_doc()['status']

    from cax.tasks.checksum import AddChecksum
    t = AddChecksum()
    t.go()

    if run_status == 'verifying':
        # AddChecksum should now have added a checksum
        assert 'checksum' in data_doc()

    else:
        # AddChecksum currently does nothing if the status isn't verifying
        # (although there is a lot of code that  for status != verifying, this is all unreachable
        # due to a status check at the top, which was probably added later...)
        assert 'checksum' not in data_doc()
