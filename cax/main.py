import argparse
import logging
import os
import datetime
import time

from cax import __version__
from cax import config, qsub

import pax

from cax.tasks import checksum, clear, data_mover, process, process_hax, filesystem
from cax.tasks import corrections


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
                        help="Select a single run using the run number")
    parser.add_argument('--name', type=str,
                        help="Select a single run using the run name")
    parser.add_argument('--host', type=str,
                        help="Host to pretend to be")


    args = parser.parse_args()

    print(args.run, config.get_hostname())

    if args.host:
        config.HOST = args.host

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
        corrections.AddElectronLifetime(),  # Add electron lifetime to run, which is just a function of calendar time
        corrections.AddGains(), #  Adds gains to a run, where this is computed using slow control information
        #corrections.AddSlowControlInformation(),  
        data_mover.CopyPull(), # Download data through e.g. scp to this location
        data_mover.CopyPush(),  # Upload data through e.g. scp or gridftp to this location where cax running
        checksum.CompareChecksums(),  # See if local data corrupted
        checksum.AddChecksum(),  # Add checksum for data here so can know if corruption (useful for knowing when many good copies!)
        clear.RetryStalledTransfer(),  # If data transferring e.g. 48 hours, probably cax crashed so delete then retry
        clear.RetryBadChecksumTransfer(),  # If bad checksum for local data and can fetch from somewhere else, delete our copy
        filesystem.SetPermission(),  # Set any permissions (primarily for Tegner) for new data to make sure analysts can access
        clear.BufferPurger(),  # Clear old data at some locations as specified in cax.json
        process.ProcessBatchQueue(),  # Process the data with pax
        process_hax.ProcessBatchQueueHax()  # Process the data with hax
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
                if args.name is not None:
                    task.go(args.name)
                else:
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
    # Command line arguments setup
    parser = argparse.ArgumentParser(description="Submit cax tasks to batch queue.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--run', type=int,
                        help="Select a single run")
    parser.add_argument('--start', type=int,
                        help="Select a starting run")

    args = parser.parse_args()

    run_once = args.once

    config_arg = ''
    if args.config_file:
        if not os.path.isfile(args.config_file):
            logging.error("Config file %s not found", args.config_file)
        else:
            logging.info("Using custom config file: %s",
                         args.config_file)
            config_arg = '--config ' + os.path.abspath(args.config_file)

    # Setup logging
    cax_version = 'cax_v%s - ' % __version__
    logging.basicConfig(filename='massive_cax.log',
                        level=logging.INFO,
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')

    # Check Mongo connection
    config.mongo_password()

    # Establish mongo connection
    collection = config.mongo_collection()
    sort_key = (('start', -1),
                ('number', -1),
                ('detector', -1),
                ('_id', -1))

    dt = datetime.timedelta(days=1)
    t0 = datetime.datetime.utcnow() - 2*dt


    while True: # yeah yeah
        query = {}

        t1 = datetime.datetime.utcnow()
        if t1 - t0 < dt:
            logging.info("Iterative mode")

            # See if there is something to do
            query['start'] = {'$gt' : t0}

            logging.info(query)
        else:
            logging.info("Full mode")
            t0 = t1

        docs = list(collection.find(query,
                                    sort=sort_key,
                                    projection=['start', 'number','name',
                                                'detector', '_id']))

        for doc in docs:

            if args.run:
                if args.run != doc['number']:
                    continue

            if args.start:
                if args.start < doc['number']:
                    continue

            job_name = ''
            if doc['detector'] == 'tpc':
                job_name = str(doc['number'])
            elif doc['detector'] == 'muon_veto':
                job_name = doc['name']

            job = dict(command='cax --once --run {number} '+config_arg,
                       number=job_name,
                           )

            script = config.processing_script(job)

            if 'cax_%s_v%s' % (job_name, pax.__version__) in qsub.get_queue():
                logging.debug('Skip: cax_%s_v%s job exists' % (job_name, pax.__version__))
                continue

            while qsub.get_number_in_queue() > (500 if config.get_hostname() == 'midway-login1' else 30):
                logging.info("Speed break 60s because %d in queue" % qsub.get_number_in_queue())
                time.sleep(60)


            print(script)
            qsub.submit_job(script)

            logging.debug("Pace by 1s")
            time.sleep(1)  # Pace 1s for batch queue

        if run_once:
            break
        #else:
        #    pace = 5
        #    logging.info("Done, waiting %d minutes" % pace)
        #    time.sleep(60*pace) # Pace 5 minutes


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
