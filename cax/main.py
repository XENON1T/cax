from cax.tasks import checksum, clear, copy
import os
import sys
import logging
import time
from cax.config import password

import daemonocle

def main2():
    password()  # Check password specified

    logging.basicConfig(filename='example.log',
                        level=logging.DEBUG,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info('Daemon is starting')

    tasks = [checksum.AddChecksum(),
             checksum.CompareChecksums(),
             clear.ClearDAQBuffer(),
             copy.SCPPush()]

    while True:
        for task in tasks:
            logging.info("Executing %s." % task.__class__.__name__)
            task.go()


        logging.debug('Sleeping.')
        time.sleep(10)

def main():
    password()  # Check password specified

    daemon = daemonocle.Daemon(worker=main,
                               pidfile=os.path.join(os.path.expanduser("~"),
                                                    'cax.pid'))
    daemon.do_action(sys.argv[1])

if __name__ == '__main__':
    main2()
