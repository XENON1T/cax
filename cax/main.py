import argparse
import logging
import os.path
import time

from cax import __version__
from cax import config, qsub
from cax.tasks import checksum, clear, data_mover, process, filesystem, purity

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
    parser.add_argument('--run', type=int,
                        help="Select a single run")
    parser.add_argument('--host', type=str,
                        help="Host to pretend to be")


    args = parser.parse_args()


    if args.host:
        config.HOST = args.host

    print(args.run, config.get_hostname())

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
        checksum.AddChecksum(),
        purity.AddElectronLifetime(),
        process.ProcessBatchQueue(),
        data_mover.CopyPull(),
        data_mover.CopyPush(),
        checksum.CompareChecksums(),
        checksum.AddChecksum(),
        clear.RetryStalledTransfer(),
        clear.RetryBadChecksumTransfer()
    ]

    # Raises exception if unknown host
    config.get_config()

    user_tasks = config.get_task_list()

    while True:
        for task in tasks:
            name = task.__class__.__name__

            # Skip tasks that user did not specify
            if user_tasks and name not in user_tasks:
                continue

            logging.info("Executing %s." % name)

            try:
                task.go(args.run)
            except Exception as e:
                logging.fatal("Exception caught from task %s" % name,
                              exc_info=True)
                logging.exception(e)
                raise

        # Decide to continue or not
        if run_once:
            break
        else:
            logging.info('Sleeping.')
            time.sleep(60)


def massive():
    parser = argparse.ArgumentParser(description="Submit cax tasks to batch queue.")
    parser.add_argument('--delete', action='store_true',
                        help="Delete strays (default: false)")

    args = parser.parse_args()
    config.mongo_password()

    query = {'detector': 'tpc'}
    for doc in config.mongo_collection().find(query):
        job = dict(command='cax --once --run {number}',
                    number=doc['number'])
        script = config.processing_script(job)

        if 'cax_%d_head' % doc['number'] in qsub.get_queue():
            print("Skip if exists")
            continue
        
        while qsub.get_number_in_queue() > 100:
            print("Speed break because %d in queue" % qsub.get_number_in_queue())
            time.sleep(60)

            
        print(script)
        qsub.submit_job(script)

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

def stray():
    parser = argparse.ArgumentParser(description="Find stray files.")
    parser.add_argument('--delete', action='store_true',
                        help="Delete strays (default: false)")

    args = parser.parse_args()
    config.mongo_password()

    filesystem.FindStrays().go()

def status():
    #Ask the database for the actual status of the file or folder:
    
    parser = argparse.ArgumentParser(description="Check the database status")
    
    parser.add_argument('--node', type=str, required=True,
                        help="Select the node")
    parser.add_argument('--status', type=str, required=True,
                        help="Which status should be asked: error, transferred, none, ")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")

    args = parser.parse_args()

    database_log = not args.disable_database_update
    node = args.node
    status = args.status
    
    # Setup logging
    cax_version = 'cax_v%s - ' % __version__
    logging.basicConfig(filename='status.log',
                        level="INFO",
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('Start: Ask for Status')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel("INFO")

    # set a format which is simpler for console use

    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    

    # Set information to update the run database
    config.set_database_log(database_log)
    config.mongo_password()
    
    filesystem.StatusSingle(args.node, args.status).go()

if __name__ == '__main__':
    main()
