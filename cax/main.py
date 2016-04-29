import logging
import time

from cax.config import mongo_password
from cax.tasks import checksum, clear, data_mover#, process

def single():
    main(run_once = True)

def main(run_once = False):
    # Check passwords and API keysspecified
    mongo_password()

    # Setup logging
    logging.basicConfig(filename='cax.log',
                        level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info('Daemon is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()

    console.setLevel(logging.INFO)

    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    tasks = [#process.ProcessBatchQueue(),
             data_mover.CopyPush(),
             #data_mover.CopyPull(),
             #checksum.AddChecksum(),
             #checksum.CompareChecksums(),
             #clear.ClearDAQBuffer(),
             #clear.AlertFailedTransfer(),
             ]

    while True:
        for task in tasks:
            logging.info("Executing %s." % task.__class__.__name__)
            task.go()

        # Decide to continue or not
        if run_once:
            break
        else:
            logging.debug('Sleeping.')
            time.sleep(60)


if __name__ == '__main__':
    main()
