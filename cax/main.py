import argparse
import logging
import time
import argparse
import os.path

from cax.config import mongo_password, set_json, get_task_list
from cax.tasks import checksum, clear, data_mover, process

def single():
    raise RuntimeError("cax-single has been removed: use cax --once instead")


def main():
    parser = argparse.ArgumentParser(description="Copying All kinds of XENON1T data.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--config', action='store', dest='config_file',
                        help="Load a custom .json config file into cax")    

    args = parser.parse_args()

    run_once = args.once

    # Check passwords and API keysspecified
    mongo_password()

    # Get specified cax.json configuration file for cax:
    caxjson_config = args.config_file
    if caxjson_config:
        if not os.path.isfile( caxjson_config ):
            logging.error("Config file %s not found", caxjson_config)
        else:
            #logging.info("Using custom config file: %s", caxjson_config) # seems to kill rest of output...
            set_json( caxjson_config )

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

    tasks = [process.ProcessBatchQueue(),
             data_mover.CopyPush(),
             data_mover.CopyPull(),
             checksum.AddChecksum(),
             checksum.CompareChecksums(),
             clear.ClearDAQBuffer(),
             clear.RetryStalledTransfer(),
             clear.RetryBadChecksumTransfer(),
             ]

    user_tasks = get_task_list()

    while True:
        for task in tasks:

            # Skip tasks that user did not specify
            if user_tasks and task.__class__.__name__ not in user_tasks:
                continue

            logging.info("Executing %s." % task.__class__.__name__)
            task.go()

        # Decide to continue or not
        if run_once:
            break
        else:
            logging.info('Sleeping.')
            time.sleep(60)


if __name__ == '__main__':
    main()
