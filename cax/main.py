import argparse
import logging
import os.path
import time

from cax import __version__
from cax import config
from cax.tasks import checksum, clear, data_mover, process, filesystem


def main():
    parser = argparse.ArgumentParser(description="Copying All kinds of XENON1T "
                                                 "data.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--log', dest='log', type=str, default='info',
                        help="Logging level e.g. debug")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")

    args = parser.parse_args()

    log_level = getattr(logging, args.log.upper())
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % args.log)

    run_once = args.once
    database_log = not args.disable_database_update

    # Set information to update the run database 
    config.set_database_log(database_log)

    # Check passwords and API keysspecified
    config.mongo_password()

    # Setup logging
    cax_version = 'cax_v%s - ' % __version__
    logging.basicConfig(filename='cax.log',
                        level=log_level,
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('Daemon is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(log_level)

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
            config.set_json(args.config_file)

    tasks = [
        process.ProcessBatchQueue(),
        data_mover.SCPPush(),
        data_mover.SCPPull(),
        checksum.AddChecksum(),
        checksum.CompareChecksums(),
        clear.ClearDAQBuffer(),
        clear.RetryStalledTransfer(),
        clear.RetryBadChecksumTransfer(),
    ]

    # Raises exception if unknown host
    config.get_config()

    user_tasks = config.get_task_list()

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


def move():
    parser = argparse.ArgumentParser(description="Move single file and notify"
                                                 " the run database.")
    parser.add_argument('--input', type=str, required=True,
                        help="Location of file or folder to be moved")
    parser.add_argument('--output', type=str, required=True,
                        help="Location file should be moved to.")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")

    args = parser.parse_args()

    database_log = not args.disable_database_update

    # Set information to update the run database
    config.set_database_log(database_log)
    config.mongo_password()

    filesystem.RenameSingle(args.input,
                            args.output).go()


def remove():
    parser = argparse.ArgumentParser(description="Remove data and notify"
                                                 " the run database.")
    parser.add_argument('--location', type=str, required=True,
                        help="Location of file or folder to be removed")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")

    args = parser.parse_args()

    database_log = not args.disable_database_update

    # Set information to update the run database
    config.set_database_log(database_log)
    config.mongo_password()

    filesystem.RemoveSingle(args.location).go()


if __name__ == '__main__':
    main()
