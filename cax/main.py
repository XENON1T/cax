import argparse
import logging
import os
import datetime
import time
import subprocess

from cax import __version__
from cax import config, qsub

import pax

from cax.tasks import checksum, clear, data_mover, process, process_hax, filesystem, tsm_mover, rucio_mover

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
    parser.add_argument('--ncpu', type=int, default=1,
                        help="Number of CPU per job")

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

    ncpu = args.ncpu
    config.NCPU = ncpu

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
        corrections.AddDriftVelocity(), #  Adds drift velocity to the run, also computed from slow control info
        #corrections.AddSlowControlInformation(),  
        data_mover.CopyPush(),  # Upload data through e.g. scp or gridftp to this location where cax running
        #tsm_mover.AddTSMChecksum(), # Add forgotten Checksum for runDB for TSM client.
	checksum.CompareChecksums(),  # See if local data corrupted
        clear.RetryStalledTransfer(),  # If data transferring e.g. 48 hours, probably cax crashed so delete then retry
        clear.RetryBadChecksumTransfer(),  # If bad checksum for local data and can fetch from somewhere else, delete our copy

        data_mover.CopyPull(), # Download data through e.g. scp to this location
        checksum.AddChecksum(),  # Add checksum for data here so can know if corruption (useful for knowing when many good copies!)

        filesystem.SetPermission(),  # Set any permissions (primarily for Tegner) for new data to make sure analysts can access
        # clear.BufferPurger(),  # Clear old data at some locations as specified in cax.json
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
    parser.add_argument('--stop', type=int,
                        help="Select the last run")
    parser.add_argument('--tag', type=str,
                        help="Select the tag")
    parser.add_argument('--ncpu', type=int, default=1,
                        help="Number of CPU per job")
    parser.add_argument('--mem_per_cpu', type=int, default=2000,
                        help="Amount of MB of memory per cpu per job")
    parser.add_argument('--walltime', type=str, default="48:00:00",
                        help="Total walltime of job")
    parser.add_argument('--partition', type=str,
                        help="Select the cluster partition")
    parser.add_argument('--reservation', type=str,
                        help="Select the reservation")

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

    tag = ''
    if args.tag:
        tag = args.tag
        #logging.info("Running only on tag: ", str(tag))

    ncpu = args.ncpu
    config.NCPU = ncpu

    mem_per_cpu = args.mem_per_cpu

    walltime = args.walltime

    partition = None
    qos = None
    if args.partition:
        partition = args.partition

        if partition == 'xenon1t':
            qos = 'xenon1t'

        elif partition == 'kicp':
            qos = 'xenon1t-kicp'

        #else:  # logging not working...
        #    logging.error("Unkown partition", partition)

    reservation = None
    if args.reservation:
        reservation = args.reservation


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

        #t1 = datetime.datetime.utcnow()
        #if t1 - t0 < dt:
        #    logging.info("Iterative mode")

        #    # See if there is something to do
        #    query['start'] = {'$gt' : t0}

        #    logging.info(query)
        #else:
        #    logging.info("Full mode")
        #    t0 = t1


        if args.run:
            query['number'] = args.run

        if args.start:
            query['number'] = {'$gte' : args.start}
	
        if args.stop:
            query['number'] = {'$lte' : args.stop}

        if tag is not '':
            query['tags.name'] = str(tag)

        docs = list(collection.find(query,
                                    sort=sort_key,
                                    projection=['start', 'number','name',
                                                'detector', '_id']))

        for doc in docs:

            job_name = ''
            
            if doc['detector'] == 'tpc':
                job_name = str(doc['number'])
                job = dict(command='cax --once --run {number} '+config_arg+' --ncpu '+str(ncpu),
                           number=int(job_name), ncpus=ncpu)
            elif doc['detector'] == 'muon_veto':
                job_name = doc['name']
                job = dict(command='cax --once --name {number} '+config_arg+' --ncpu '+str(ncpu),
                           number=job_name, ncpus=ncpu)

            job['mem_per_cpu'] = mem_per_cpu

            job['time'] = walltime

            if partition is not None:
                job['partition'] = '\n#SBATCH --partition='+partition

            job['extra'] = ''
            if qos is not None:
                job['extra'] += '\n#SBATCH --qos='+qos

            if reservation is not None:
                job['extra'] += '\n#SBATCH --reservation='+reservation

            script = config.processing_script(job)

            if 'cax_%s_v%s' % (job_name, pax.__version__) in qsub.get_queue():
                logging.debug('Skip: cax_%s_v%s job exists' % (job_name, pax.__version__))
                continue

            while qsub.get_number_in_queue(partition=partition) > (500 if config.get_hostname() == 'midway-login1' else 30):
                logging.info("Speed break 60s because %d in queue" % qsub.get_number_in_queue(partition=partition))
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
    parser.add_argument('--run', type=int, required= True,
                        help="Run number to process")

    args = parser.parse_args()

    database_log = not args.disable_database_update

    # Set information to update the run database
    config.set_database_log(database_log)
    config.mongo_password()

    filesystem.RemoveSingle(args.location).go(args.run)

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

#Rucio Stuff
def ruciax():
    parser = argparse.ArgumentParser(description="Copying All kinds of XENON1T "
                                                 "data.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    
    parser.add_argument('--rucio-rule', type=str,
                        dest='config_rule',
                        help="Load the a rule file") 
    
    parser.add_argument('--log', dest='log', type=str, default='info',
                        help="Logging level e.g. debug")
    
    parser.add_argument('--log-file', dest='logfile', type=str, default='ruciax.log',
                        help="Specify a certain logfile")
    
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")
    
    parser.add_argument('--run', type=int,
                        help="Select a single run by its number")
    
    parser.add_argument('--name', type=str,
                        help="Select a single run by its name")
    
    parser.add_argument('--host', type=str,
                        help="Host to pretend to be")
    
    #parser for rucio arguments
    parser.add_argument('--rucio-scope', type=str, dest='rucio_scope',
                        help="Rucio: Choose your scope")

    parser.add_argument('--rucio-rse', type=str, dest='rucio_rse',
                        help="Rucio: Choose your rse")

    parser.add_argument('--rucio-upload', type=str, dest='rucio_upload',
                        help="Rucio: Select a data file or data set")
    
    
    args = parser.parse_args()
   
    #This one is mandatory: hardcoded science run number!
    config.set_rucio_campaign("000")

    log_level = getattr(logging, args.log.upper())
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % args.log)

    run_once = args.once
    database_log = not args.disable_database_update

    # Set information to update the run database 
    config.set_database_log(database_log)

    # Check passwords and API keysspecified
    config.mongo_password()

    # Set information for rucio transfer rules (config file)
    config.set_rucio_rules( args.config_rule)

    # Setup logging
    cax_version = 'ruciax_v%s - ' % __version__
    logging.basicConfig(filename=args.logfile,
                        level=log_level,
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    logging.info('Daemon is starting')
    logging.info('Logfile: %s', args.logfile)
    
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
        data_mover.CopyPull(),
        data_mover.CopyPush(),
        rucio_mover.RucioRule()
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

def massiveruciax():
    # Command line arguments setup
    parser = argparse.ArgumentParser(description="Submit ruciax tasks to batch queue.")

    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--on-disk-only', action='store_true',
                        dest='on_disk_only',
                        help="Select only runs which are stored at the data analysis facility anyways")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--run', type=int,
                        help="Select a single run")
    parser.add_argument('--from-run', dest='from_run', type=int, 
                        help="Choose: run number start")
    parser.add_argument('--to-run', dest='to_run', type=int, 
                        help="Choose: run number end")
    parser.add_argument('--log-file', dest='logfile', type=str, default='massive_rucio.log',
                        help="Specify a certain logfile")
    parser.add_argument('--rucio-rule', type=str,
                        dest='config_rule',
                        help="Load the a rule file") 
    

    args = parser.parse_args()

    run_once = args.once

    #Check on from-run/to-run option:
    beg_run = -1
    end_run = -1
    run_window = False
    run_window_lastruns = False
    
    if args.from_run == None and args.to_run == None:
      pass
    elif (args.from_run != None and args.to_run == None) or (args.from_run == None and args.to_run != None):
      logging.info("Select (tpc) runs between %s and %s", args.from_run, args.to_run)
      logging.info("Make a full selection!")
    elif args.from_run != None and args.to_run != None and args.from_run > 0 and args.to_run > 0 and args.from_run > args.to_run:
      logging.info("The last run is smaller then the first run!")
      logging.inof("--> Ruciax exits here")
      exit()
    elif args.from_run != None and args.to_run != None and args.from_run > 0 and args.to_run > 0 and args.from_run < args.to_run:
      logging.info("Start (tpc) runs between %s and %s", args.from_run, args.to_run)
      run_window = True
      beg_run = args.from_run
      end_run = args.to_run
    elif args.from_run != None and args.to_run != None and args.from_run > 0 and args.to_run == -1:
      logging.info("Start (tpc) runs between %s and to last", args.from_run)
      run_window_lastruns = True
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
            config_arg = '--config ' + os.path.abspath(args.config_file)
      
    # Setup logging
    log_path = {"xe1t-datamanager": "/home/xe1ttransfer/rucio_log",
               "midway-login1": "/home/bauermeister/rucio_log",
               "tegner-login-1": "/afs/pdc.kth.se/home/b/bobau/rucio_log",
               "login": "/home/bauermeister/rucio_log"}
    
    if not os.path.exists(log_path[config.get_hostname()]):
        os.makedirs(log_path[config.get_hostname()])
    cax_version = 'massive_ruciax_v%s - ' % __version__
    logging.basicConfig(filename="{logp}/{logf}".format(logp=log_path[config.get_hostname()], logf=args.logfile),
                        level=logging.INFO,
                        format=cax_version + '%(asctime)s [%(levelname)s] ' '%(message)s')
    
    # Check Mongo connection
    config.mongo_password()
    
    #Define additional file to define the rules:
    abs_config_rule = os.path.abspath( args.config_rule )
    if args.config_rule:
      logging.info("Rucio Rule File: %s", abs_config_rule)
      rucio_rule = "--rucio-rule {rulefile}".format( rulefile=abs_config_rule )
    else:
      rucio_rule = ""
      
    # Establish mongo connection
    collection = config.mongo_collection()
    sort_key = (('number', -1),
                ('detector', -1),
                ('_id', -1))

    #Construct the pre-basic bash script(s) from rucio_mover.RucioConfig()
    RucioBashConfig = rucio_mover.RucioConfig()
    
    while True: # yeah yeah
        #query = {'detector':'tpc'}
        query = {}
        
        docs = list(collection.find(query))
        
        for doc in docs:
            
            #Select a single run for rucio upload (massive-ruciax -> ruciax)
            if args.run:
              if args.run != doc['number']:
                continue
            
            #Rucio upload only of certain run numbers in a sequence
            if int(doc['number']) < beg_run or int(doc['number']) > end_run and run_window == True:
              continue
            
            if int(doc['number']) < beg_run and run_window_lastruns == True:
              continue
          
            #Double check if a 'data' field is defind in doc
            if 'data' not in doc:
              continue
            
            #Double check that rucio uploads are only triggered when data exists at the host
            host_data = False
            for idoc in doc['data']:
              if idoc['host'] == config.get_hostname():
                host_data = True
                break
            if host_data == False and args.on_disk_only == True:
              continue
            
            #Get the local time:
            local_time = time.strftime("%Y%m%d_%H%M%S", time.localtime())
            
            #Detector choice
            run = ""
            runlogfile = ""
            if doc['detector'] == 'tpc':
              run = "--run {number}".format(number=doc['number'])
              runlogfile = "--log-file {log_path}/ruciax_log_{number}_{timestamp}.txt".format(
                            log_path=log_path[config.get_hostname()],
                            number=doc['number'],
                            timestamp=local_time)
                  
            elif doc['detector'] == 'muon_veto':
              run = "--name {number}".format(number=doc['name'])
              runlogfile = "--log-file {log_path}/ruciax_log_{number}_{timestamp}.txt".format(
                            log_path=log_path[config.get_hostname()],
                            number=doc['number'],
                            timestamp=local_time)
            
            #Define the job:
            job = "{conf} {run} {rucio_rule} {runlogfile}".format(
                  conf=config_arg,
                  run=run,
                  rucio_rule=rucio_rule,
                  runlogfile=runlogfile)
            
            #start the time for an upload:
            time_start = datetime.datetime.utcnow()
            
            #Create the command
            command="""
ruciax --once {job}
""".format(job=job)

            #pre_bash_command =  RucioBashConfig.get_config( config.get_hostname() ).format(
                                    #account=config.get_config("rucio-catalogue")['rucio_account'] 
                                    #)
            pre_bash_command = RucioBashConfig.load_host_config( config.get_hostname(), "py3" ).format(
                                    account=config.get_config("rucio-catalogue")['rucio_account'] 
                                    ) 
            
            command = pre_bash_command + command
                        
            logging.info(command)
            
            
            #Submit the command
            sc = qsub.create_script(command)
            execute = subprocess.Popen( ['sh', sc.name] , 
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, shell=False )
            stdout_value, stderr_value = execute.communicate()
            stdout_value = stdout_value.decode("utf-8")
            stdout_value = stdout_value.split("\n")
            
            #Return command output:
            for i in stdout_value:
              logging.info('massive-ruciax: %s', i)
            
            #Manage the upload time:
            time_end = datetime.datetime.utcnow()
            diff = time_end-time_start
            dd = divmod(diff.total_seconds(), 60)
            
            #delete script:
            qsub.delete_script( sc )
            
            logging.info("+--------------------------->>>")
            logging.info("| Summary: massive-ruciax for run/name: %s/%s", doc['number'], doc['name'] )
            logging.info("| Configuration script: %s", runlogfile)
            logging.info("| Rucio-rule script: %s", abs_config_rule)
            logging.info("| Run time of ruciax: %s min %s", str(dd[0]), str(dd[1]) )
            logging.info("+------------------------------------------------->>>")
        if run_once:
          break
        else:
          logging.info('Sleeping.')
          time.sleep(60)  

    
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
                "midway-login1": "n/a",
                "tegner-login-1": "/afs/pdc.kth.se/home/b/bobau/tsm_log"}
    
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
    parser.add_argument('--log-path', type=str,
                        dest='log_path',
                        help="Point to the directory where the log files are stored.")
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
      a = tsm_mover.TSMLogFileCheck(args.log_path)

    elif args.monitor == "database":
      """load the data base watcher class"""
      total_uploaded_amount = 0
      total_uploaded_datasets = 0

      while True: # yeah yeah

        query = {}

        docs = list(collection.find(query))

        for doc in docs:

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

def ruciax_status():
    parser = argparse.ArgumentParser(description="Allow to check the run database for rucio entries")

    parser.add_argument('--location', type=str, required=False,
                        dest='location', default=None,
                        help="Location of file or folder to be removed")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")
    parser.add_argument('--run', type=int, required=False,
                        dest='run', default=None,
                        help="Select a single run by number")
    parser.add_argument('--name', type=str, required=False,
                        dest='name', default=None,
                        help="Select a single run by name")

    parser.add_argument('--mode', type=str, required=True,
                        dest='mode',
                        help="Choose the kind of test you want to run")

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

    if args.mode == "DoubleEntries":
      print("Check if there are more than one entries in the runDB")
      filesystem.RuciaxTest(args.mode, args.location).go(number_name)

def remove_from_rucio():
    parser = argparse.ArgumentParser(description="Remove data and notify"
                                                 " the run database.")
    parser.add_argument('--location', type=str, required=True,
                        help="Location of file or folder to be removed")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")
    parser.add_argument('--run', type=int, required=False,
                        help="Select a single run by number")
    parser.add_argument('--name', type=str, required=False,
                        help="Select a single run by name")
    parser.add_argument('--status', type=str, required=False,
                        help="Select the status in the runDB")
    
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

    filesystem.RemoveRucioEntry(args.location, args.status).go(number_name)

def ruciax_purge():
    #Ask the database for the actual status of the file or folder:
    
    parser = argparse.ArgumentParser(description="Check the database status")
    
    parser.add_argument('--run', type=int, required=False,
                        help="Select a single run by number")
    parser.add_argument('--name', type=str, required=False,
                        help="Select a single run by name")    
    parser.add_argument('--purge', type=bool, required=False,
                        dest='purge', default=False,
                        help="Activate purge modus [True]")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")

    args = parser.parse_args()

    database_log = not args.disable_database_update

    # Setup logging
    cax_version = 'cax_v%s - ' % __version__
    logging.basicConfig(filename='ruciax_purge.log',
                        level="INFO",
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    
    logging.info('Purge raw data sets - Handle with care!')

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
    
    number_name = None
    if args.name is not None:
      number_name = args.name
    else:
      number_name = args.run
    
    rucio_mover.RucioPurge(args.purge).go(number_name)

def ruciax_download():
    #Ask the database for the actual status of the file or folder:
    
    parser = argparse.ArgumentParser(description="Check the database status")
    
    parser.add_argument('--run', type=int, required=False,
                        help="Select a single run by number")
    parser.add_argument('--name', type=str, required=False,
                        help="Select a single run by name")    
    parser.add_argument('--location', type=str, required=False,
                        dest='location',
                        help="Select a single run by rucio location") 
    parser.add_argument('--type', type=str, required=False,
                        dest='data_type',
                        help="Select what kind of data you want to download [raw/processed]")    
    parser.add_argument('--rse', type=str, required=False,
                        dest='data_rse',
                        help="Select a specific rucio storage endpoint for download")  
    parser.add_argument('--dir', type=str, required=True,
                        dest='data_dir',
                        help="Select a download directory")
    parser.add_argument('--restore', type=bool, required=False, default=False,
                        dest='restore',
                        help="Enable restore mode to recover data from the rucio catalogue")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--overwrite', action='store_true',
                        help="Data are overwritten at the host when this opition is activated.")
    parser.add_argument('--disable_database_update', action='store_true',
                        help="Disable the update function the run data base")
    args = parser.parse_args()

    database_log = not args.disable_database_update

    # Setup logging
    cax_version = 'cax_v%s - ' % __version__
    logging.basicConfig(filename='ruciax_download.log',
                        level="INFO",
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    
    logging.info('Ruciax - The data set downloader')

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
    
    if args.config_file:
        if not os.path.isfile(args.config_file):
            logging.error("Config file %s not found", args.config_file)
        else:
            logging.info("Using custom config file: %s",
                         args.config_file)
            config.set_json(args.config_file)
    
    number_name = None
    if args.name is not None:
      number_name = args.name
    else:
      number_name = args.run
    
    rucio_mover.RucioDownload(args.data_rse, args.data_dir, args.data_type, args.restore, args.location, args.overwrite).go(number_name)

def ruciax_locator():
    #Ask the database for the actual status of the file or folder:
    
    parser = argparse.ArgumentParser(description="Check the database status")
    
    parser.add_argument('--run', type=int, required=False,
                        help="Select a single run by number")
    parser.add_argument('--name', type=str, required=False,
                        help="Select a single run by name")    
    parser.add_argument('--rse', type=str, required=False,
                        dest='rse', action='append',
                        help="Select an rucio storage element")
    parser.add_argument('--copies', type=int, required=False,
                        dest='copies',
                        help="Select how many copies are requested")
    parser.add_argument('--status', type=str, required=False,
                        dest='status',
                        help="Select the requested status")
    parser.add_argument('--config', action='store', type=str,
                        dest='config_file',
                        help="Load a custom .json config file into cax")
    parser.add_argument('--method', type=str, required=True,
                        dest='method',
                        help="Select method: [SingleRun] (--run) | [Status] (--status) | [CheckRSEMultiple] (--rse) |  [CheckRSESingle] (--rse) | [MultiCopies] (--copies) | [ListSingleRules] (--run/--name)")
    
    args = parser.parse_args()


    # Setup logging
    cax_version = 'cax_v%s - ' % __version__
    logging.basicConfig(filename='ruciax_locator.log',
                        level="INFO",
                        format=cax_version + '%(asctime)s [%(levelname)s] '
                                             '%(message)s')
    
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
    config.mongo_password()
    
    if args.config_file:
      if not os.path.isfile(args.config_file):
        logging.error("Config file %s not found", args.config_file)
      else:
        logging.info("Using custom config file: %s",
                     args.config_file)
        config.set_json(args.config_file)
    
    number_name = None
    if args.name is not None:
      number_name = args.name
    else:
      number_name = args.run
    
    rucio_mover.RucioLocator(args.rse, args.copies, args.method, args.status).go(number_name)

if __name__ == '__main__':
    main()
