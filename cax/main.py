import logging
import time

from cax.config import password
from cax.tasks import checksum, clear, copy


def main():
    password()  # Check password specified

    logging.basicConfig(filename='example.log',
                        level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info('Daemon is starting')

    tasks = [checksum.AddChecksum(),
             checksum.CompareChecksums(),
             #clear.ClearDAQBuffer(),
             copy.SCPPush()]

    while True:
        for task in tasks:
            logging.info("Executing %s." % task.__class__.__name__)
            task.go()

        logging.debug('Sleeping.')
        time.sleep(10)


if __name__ == '__main__':
    main()
