import logging
import time

from cax.config import mongo_password, pagerduty_api_key
from cax.tasks import checksum, clear, copy

def single():
    main(run_once = True)

def main(run_once = False):
    # Check passwords and API keysspecified
    mongo_password()
    pagerduty_api_key()

    # Setup logging
    logging.basicConfig(filename='cax.log',
                        level=logging.DEBUG,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info('Daemon is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()

    console.setLevel(logging.WARNING)

    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    tasks = [checksum.AddChecksum(),
             checksum.CompareChecksums(),
             #clear.ClearDAQBuffer(),
             clear.AlertFailedTransfer(),
             copy.SCPPush()]

    while True:
        for task in tasks:
            logging.info("Executing %s." % task.__class__.__name__)
            task.go()

        # Decide to continue or not
        if run_once:
            break
        else:
            logging.debug('Sleeping.')
            time.sleep(10)


if __name__ == '__main__':
    main()
