import argparse
import logging
import os.path
import time

from cax.config import mongo_password, set_json, get_task_list, get_config
from cax.tasks import checksum, clear, data_mover, process


def main():
    parser = argparse.ArgumentParser(
        description="Copying All kinds of XENON1T data.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--config', action='store', dest='config_file',
                        help="Load a custom .json config file into cax")

    args = parser.parse_args()

    run_once = args.once

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

    # Get specified cax.json configuration file for cax:
    if args.config_file:
        if not os.path.isfile(args.config_file):
            logging.error("Config file %s not found", args.config_file)
        else:
            logging.info("Using custom config file: %s",
                         args.config_file)
            set_json(args.config_file)

    tasks = [process.ProcessBatchQueue(),
             data_mover.SCPPush(),
             data_mover.SCPPull(),
             checksum.AddChecksum(),
             checksum.CompareChecksums(),
             clear.ClearDAQBuffer(),
             clear.RetryStalledTransfer(),
             clear.RetryBadChecksumTransfer(),
             ]

    # Raises exception if unknown host
    get_config()
 
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
