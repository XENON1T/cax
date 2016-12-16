import argparse
import logging
import os
import datetime
import time
import subprocess

from cax import __version__
from cax import config, qsub

import pax

from cax.tasks import checksum, clear, data_mover, process, process_hax, filesystem, tsm_mover

from cax.tasks import corrections


def main():
    parser = argparse.ArgumentParser(description="Copying All kinds of XENON1T "
                                                 "data.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--version', action='store_true',
                        help="Print the cax version, then exits")

    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--log', dest='log', type=str, default='info',
                        help="Logging level e.g. debug")
    parser.add_argument('--log-file', dest='logfile', type=str, default='cax.log',
                        help="Log file")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")
    parser.add_argument('--run', type=int,
                        help="Select a single run using the run number")
    parser.add_argument('--name', type=str,
                        help="Select a single run using the run name")
    parser.add_argument('--host', type=str,
                        help="Host to pretend to be")

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit()

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
    logging.basicConfig(filename=args.logfile,
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
        #tsm_mover.AddTSMChecksum(), # Add forgotten Checksum for runDB for TSM client. 
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
    parser.add_argument('--version', action='store_true',
                        help="Print the cax version, then exits")

    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--run', type=int,
                        help="Select a single run")
    parser.add_argument('--start', type=int,
                        help="Select a starting run")

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit()

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
    t0 = datetime.datetime.utcnow() - 2 * dt


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


        if args.run:
            query['number'] = args.run

        if args.start:
            query['number'] = {'$gte' : args.start}

        docs = list(collection.find(query,
                                    sort=sort_key,
                                    projection=['start', 'number','name',
                                                'detector', '_id']))

        for doc in docs:

            job_name = ''
            if doc['detector'] == 'tpc':
                job_name = str(doc['number'])
                job = dict(command='cax --once --run {number} '+config_arg,
                           number=int(job_name))
            elif doc['detector'] == 'muon_veto':
                job_name = doc['name']
                job = dict(command='cax --once --name {number} '+config_arg,
                           number=job_name)

            script = config.processing_script(job)

            if 'cax_%s_v%s' % (job_name, pax.__version__) in qsub.get_queue():
                logging.debug('Skip: cax_%s_v%s job exists' % (job_name, pax.__version__))
                continue

            while qsub.get_number_in_queue() > (500 if config.get_hostname() == 'midway-login1' else 30):
                logging.info("Speed break 60s because %d in queue" % qsub.get_number_in_queue())
                time.sleep(60)


            print(script)
            qsub.submit_job(script)

            logging.debug("Pace by 1 s")
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
    
    parser.add_argument('--host', type=str, required=True,
                        help="Select the host")
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
    
    filesystem.StatusSingle(args.host, args.status).go()

def remove_from_tsm():
    parser = argparse.ArgumentParser(description="Remove data and notify"
                                                 " the run database.")
    parser.add_argument('--location', type=str, required=True,
                        help="Location of file or folder to be removed")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function of the run data base")
    parser.add_argument('--run', type=int, required=False,
                        help="Select a single run by number")
    parser.add_argument('--name', type=str, required=False,
                        help="Select a single run by name")
    
    args = parser.parse_args()

    database_log = not args.disable_database_update

    # Set information to update the run database
    config.set_database_log(database_log)
    config.mongo_password()
    
    number_name = None
    if args.name is not None:
      number_name = args.name
    else:
      number_name = args.run

    filesystem.RemoveTSMEntry(args.location).go(number_name)

def massive_tsmclient():
    # Command line arguments setup
    parser = argparse.ArgumentParser(description="Submit ruciax tasks to batch queue.")

    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just once, then exits")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--run', type=int,
                        help="Select a single run")
    parser.add_argument('--from-run', dest='from_run', type=int, 
                        help="Choose: run number start")
    parser.add_argument('--to-run', dest='to_run', type=int, 
                        help="Choose: run number end")
    parser.add_argument('--log', dest='log', type=str, default='INFO',
                        help="Logging level e.g. debug")
    parser.add_argument('--logfile', dest='logfile', type=str, default='massive_tsm.log',
                        help="Specify a certain logfile")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")

    args = parser.parse_args()
    
    log_level = args.log
    #if not isinstance(log_level, int):
        #raise ValueError('Invalid log level: %s' % args.log)

    run_once = args.once

    #Check on from-run/to-run option:
    beg_run = -1
    end_run = -1
    run_window = False
    if args.from_run == None and args.to_run == None:
      pass
    elif (args.from_run != None and args.to_run == None) or (args.from_run == None and args.to_run != None):
      logging.info("Select (tpc) runs between %s and %s", args.from_run, args.to_run)
      logging.info("Make a full selection!")
    elif args.from_run != None and args.to_run != None and args.from_run >= args.to_run:
      logging.info("The last run is smaller then the first run!")
    elif args.from_run != None and args.to_run != None and args.from_run < args.to_run:
      logging.info("Start (tpc) runs between %s and %s", args.from_run, args.to_run)
      run_window = True
      beg_run = args.from_run
      end_run = args.to_run
    
    
    #configure cax.json
    config_arg = ''
    if args.config_file:
        if not os.path.isfile(args.config_file):
            logging.error("Config file %s not found", args.config_file)
        else:
            logging.info("Using custom config file: %s",
                         args.config_file)
            config.set_json(args.config_file)
            config_arg = os.path.abspath(args.config_file)
      
    # Setup logging
    log_path = {"xe1t-datamanager": "/home/xe1ttransfer/tsm_log",
                "midway-login1": "n/a"}
    
    if log_path[config.get_hostname()] == "n/a":
        print("Modify the log path in main.py")
        exit()
    
    if not os.path.exists(log_path[config.get_hostname()]):
        os.makedirs(log_path[config.get_hostname()])
    cax_version = 'massive_tsm-client_v%s - ' % __version__
    logging.basicConfig(filename="{logp}/{logf}".format(logp=log_path[config.get_hostname()], logf=args.logfile),
                        level="INFO",
                        format=cax_version + '%(asctime)s [%(levelname)s] ' '%(message)s')
    
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel("INFO")

    # set a format which is simpler for console use

    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    
    # Check Mongo connection
    config.mongo_password()

    # Establish mongo connection
    collection = config.mongo_collection()
    
    sort_key = (('start', -1),
                ('number', -1),
                ('detector', -1),
                ('_id', -1))
    
    while True: # yeah yeah
        
        query = {}
        docs = list(collection.find(query))
        
        for doc in docs:
            
            #Select a single run for rucio upload (massive-ruciax -> ruciax)
            if args.run:
              if args.run != doc['number']:
                continue
            
            #Rucio upload only of certain run numbers in a sequence
            if doc['number'] < beg_run or doc['number'] > end_run and run_window == True:
              continue
            
            #Double check if a 'data' field is defind in doc (runDB entry)
            if 'data' not in doc:
              continue
            
            #Double check that tsm uploads are only triggered when data exists at the host
            host_data = False
            for idoc in doc['data']:
              if idoc['host'] == config.get_hostname():
                host_data = True
                break
            if host_data == False:
              #Do not try upload data which are not registered in the runDB
              continue
            
            #Detector choice
            local_time = time.strftime("%Y%m%d_%H%M%S", time.localtime())
            if doc['detector'] == 'tpc':
              job = "--config {conf} --run {number} --log-file {log_path}/tsm_log_{number}_{timestamp}.txt".format(
                  conf=config_arg,
                  number=doc['number'],
                  log_path=log_path[config.get_hostname()],
                  timestamp=local_time)
                
            elif doc['detector'] == 'muon_veto':
              job = "--config {conf} --name {number} --log-file {log_path}/tsm_log_{number}_{timestamp}.txt".format(
                  conf=config_arg,
                  number=doc['name'],
                  log_path=log_path[config.get_hostname()],
                  timestamp=local_time)
            
            #start the time for an upload:
            time_start = datetime.datetime.utcnow()
            
            #Create the command and execute the job only once
            command="cax --once {job}".format(job=job)
            
            #Disable runDB notifications
            if args.disable_database_update == True:
              command = command + " --disable_database_update"
            
            #command = general[config.get_hostname()]+command
                        
            logging.info("Command: %s", command)
            
            command = command.replace("\n", "")
            command = command.split(" ")
            execute = subprocess.Popen( command , 
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, shell=False )
            stdout_value, stderr_value = execute.communicate()
            stdout_value = stdout_value.decode()
      
            stdout_value = str(stdout_value).split("\n")
            stderr_value = str(stderr_value).split("\n")
      
            stdout_value.remove('') #remove '' entries from an array
            
            #Return command output:
            for i in stdout_value:
              logging.info("massive-tsm: %s", i)
            
            #Manage the upload time:
            time_end = datetime.datetime.utcnow()
            diff = time_end-time_start
            dd = divmod(diff.total_seconds(), 60)
            logging.info("Upload time: %s min %s", str(dd[0]), str(dd[1]))
            
        if run_once:
          break
        else:
          logging.info('Sleeping.')
          time.sleep(60)

def cax_tape_log_file():
    """Analyses the tsm storage"""
    parser = argparse.ArgumentParser(description="This program helps you to watch the tape backup")

    parser.add_argument('--monitor', dest='monitor', 
                        help="Select if you want to monitor database or logfile [database/logfile/checkstatus]")
    parser.add_argument('--status', type=str,
                        help="Select a status you want monitor: [transferred/transferring/error]")
    parser.add_argument('--run', type=int,
                        help="Select a single run")
    parser.add_argument('--name', type=int,
                        help="Select a single run")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    run_once = True
    args = parser.parse_args()
    
    # Check Mongo connection
    config.mongo_password()

    # Establish mongo connection
    collection = config.mongo_collection()
    sort_key = (('number', -1),
                ('detector', -1),
                ('_id', -1))
    
    if args.monitor == "logfile":
      """load the logfile watcher class"""
      a = tsm_mover.TSMLogFileCheck()
    
    elif args.monitor == "database":
      """load the data base watcher class"""
      #a = tsm_mover.TSMDatabaseCheck()


      total_uploaded_amount = 0
      total_uploaded_datasets = 0
      #while True: # yeah yeah
      while True: # yeah yeah    
        #query = {'detector':'tpc'}
        query = {}
        
        docs = list(collection.find(query))
        
        for doc in docs:
          #tsm_mover.TSMDatabaseCheck()
          
          if 'data' not in doc:
            continue
          
          nb_tsm_hosts = 0
          for idoc in doc['data']:
            if idoc['host'] == "tsm-server" and idoc['location'] != "n/a" and idoc['location'].find("tsm/") >= 0:
              nb_tsm_hosts+=1
              logging.info("Dataset: %s at host %s", idoc['location'], idoc['host'] )
              fsize = tsm_mover.TSMDatabaseCheck().get_info(idoc['location'])
              total_uploaded_datasets += 1
              logging.info( "Total file size of the individual dataset: %s mb", fsize)
              total_uploaded_amount += fsize
          
          if nb_tsm_hosts == 0:
            continue
        
        if run_once:
            break
        
      logging.info("Total amount of uploaded data: %s mb", total_uploaded_amount)
      logging.info("Total amount of uploaded data sets: %s", total_uploaded_datasets)

    elif args.monitor == "checkstatus":
      if args.status == None:
        logging.info("ATTENTION: Specify status by --status [transferred/transferring/error]")
        return 0
      
      query = {}  
      docs = list(collection.find(query))  
      number_name = None
      if args.name is not None:
        number_name = args.name
      else:
        number_name = args.run
        
      tsm_mover.TSMStatusCheck(docs, args.status).go(number_name)  

if __name__ == '__main__':
    main()
