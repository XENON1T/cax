from cax.tasks import checksum, clear, copy
import os

import logging
import time

import daemonocle

def cb_shutdown(message, code):
    logging.info('Daemon is stopping')
    logging.debug(message)

def main():
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
        break



if __name__ == '__main__':
    if os.environ.get('MONGO_PASSWORD') is None:
        raise EnvironmentError('Environmental variable MONGO_PASSWORD not set.'
                               ' This is required for communicating with the '
                               'run database.  To fix this problem, Do:'
                               '\n\n\texport MONGO_PASSWORD=xxx\n\n'
                               'Then rerun this command.')
    main()
    #daemon = daemonocle.Daemon(worker=main,
    #                           shutdown_callback=cb_shutdown,
    #                           pidfile='/var/run/daemonocle_example.pid')
    #daemon.do_action(sys.argv[1])