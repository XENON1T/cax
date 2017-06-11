"""Configuration routines
"""

import json
import logging
import os
import pax
import socket
from zlib import adler32
import pymongo

# global variable to store the specified .json config file
CAX_CONFIGURE = ''
DATABASE_LOG = True
HOST = os.environ.get("HOSTNAME") if os.environ.get("HOSTNAME") else socket.gethostname().split('.')[0]
DATA_USER_PDC = 'bobau'
DATA_GROUP_PDC = 'xenon-users'

#DETECTOR = "tpc"
API_URL = "https://xenon1t-daq.lngs.infn.it/runs_api/runs/runs/"


RUCIO_RSE = ''
RUCIO_SCOPE = ''
RUCIO_UPLOAD = None
RUCIO_CAMPAIGN = ''

PAX_DEPLOY_DIRS = {
    'midway-login1' : '/project/lgrandi/deployHQ/pax',
    'tegner-login-1': '/afs/pdc.kth.se/projects/xenon/software/pax'
}

RUCIO_RULE = ''



def mongo_password():
    """Fetch passsword for MongoDB

    This is stored in an environmental variable MONGO_PASSWORD.
    """
    mongo_pwd = os.environ.get('MONGO_PASSWORD')
    if mongo_pwd is None:
        raise EnvironmentError('Environmental variable MONGO_PASSWORD not set.'
                               ' This is required for communicating with the '
                               'run database.  To fix this problem, Do:'
                               '\n\n\texport MONGO_PASSWORD=xxx\n\n'
                               'Then rerun this command.')
    return mongo_pwd


def get_hostname():
    """Get hostname of the machine we're running on.
    """
    global HOST
    if '.' in HOST:
        HOST = HOST.split('.')[0]
    return HOST


def set_json(config):
    """Set the cax.json file at your own
    """
    global CAX_CONFIGURE
    CAX_CONFIGURE = config

def set_database_log(config):
    """Set the database update
    """
    global DATABASE_LOG
    DATABASE_LOG = config

def load():
    # User-specified config file
    if CAX_CONFIGURE:
        filename = os.path.abspath(CAX_CONFIGURE)

    # Default config file
    else:
        dirname = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(dirname, 'cax.json')

    logging.debug('Loading config file %s' % filename)

    return json.loads(open(filename, 'r').read())

def purge_settings(hostname=get_hostname()):
    return get_config(hostname).get('purge',
                                    None)

def nstream_settings(hostname=get_hostname()):
    return get_config(hostname).get('nstreams',
                                    None)

def get_cert(hostname=get_hostname()):
    return get_config(hostname).get('grid_cert',
                                    None)

def get_config(hostname=get_hostname()):
    """Returns the cax configuration for a particular hostname
    NB this currently reloads the cax.json file every time it is called!!
    """
    for doc in load():
        if doc['name'] == hostname:
            return doc
        elif hostname == "upload_tsm":
            return hostname
    raise LookupError("Unknown host %s" % hostname)


def get_transfer_options(transfer_kind='upload', transfer_method=None):
    """Returns hostnames that the current host can upload or download to.
    transfer_kind: 'upload' or 'download'
    transfer_method: is specified and not None, return only hosts with which
                     we can work using this method (e.g. scp)
    """
    try:
        transfer_options = get_config(get_hostname())[
            '%s_options' % transfer_kind]
    except LookupError:
        logging.info("Host %s has no known transfer options.",
                     get_hostname())
        return []

    if transfer_method is not None:
        transfer_options = [to for to in transfer_options
                            if get_config(to['host'])['method'] == 'method']

    return transfer_options


def get_pax_options(option_type='versions'):
    try:
        options = get_config(get_hostname())['pax_%s' % option_type]
    except LookupError as e:
        logging.info("Pax versions not specified: %s", get_hostname())
        return []

    return options


def get_dataset_list():
    try:
        options = get_config(get_hostname())['dataset_list']
    except LookupError as e:
        logging.debug("dataset_list not specified, operating on entire DB")
        return []

    logging.debug("dataset list:")
    logging.debug(options) 
    return options


def get_task_list():
    try:
        options = get_config(get_hostname())['task_list']
    except LookupError as e:
        logging.debug("task_list not specified, running all tasks")
        return []

    return options

def api_user():
    return os.environ.get("API_USER")

def api_key():
    return os.environ.get("API_KEY")

def mongo_collection(collection_name='runs_new'):
    # For the event builder to communicate with the gateway, we need to use the DAQ network address
    # Otherwise, use the internet to find the runs database
    if get_hostname().startswith('eb'):
        c = pymongo.MongoClient('mongodb://eb:%s@gw:27017/run' % os.environ.get('MONGO_PASSWORD'))
    else:
        uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
        uri = uri % os.environ.get('MONGO_PASSWORD')
        c = pymongo.MongoClient(uri,
                                replicaSet='runs',
                                readPreference='secondaryPreferred')
    db = c['run']
    collection = db[collection_name]
    return collection


def data_availability(hostname=get_hostname()):
    collection = mongo_collection()

    results = []
    for doc in collection.find({'detector': 'tpc'},
                               ['name', 'data']):
        for datum in doc['data']:
            if datum['status'] != 'transferred':
                continue
            if 'host' in datum and datum['host'] != hostname:
                continue
            results.append(doc)
    return results



def fill_args(args, default_args):
    # used in processing_script() below to assign default values to args not passed to processing_scrit()
    for key, value in default_args.items():
        if key not in args:
            #print('ARGS:', args)
            #print('KEY:', key)
            #print('value', value)
            args[key] = value

# for midway and tegner
SBATCH_TEMPLATE = """#!/bin/bash
#SBATCH --job-name={use}_{number}_{pax_version}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={ncpus}
#SBATCH --output={base}/{use}/{number}_{pax_version}/{number}_{pax_version}_%J.log
#SBATCH --error={base}/{use}/{number}_{pax_version}/{number}_{pax_version}_%J.log
#SBATCH --account={account}
#SBATCH --partition={partition}
{extra}
export PATH={anaconda}:$PATH
export JOB_WORKING_DIR={base}/{use}/{number}_{pax_version}
mkdir -p ${{JOB_WORKING_DIR}}
cd ${{JOB_WORKING_DIR}}
rm -f pax_event_class*
source activate pax_{pax_version}
HOSTNAME={host}
{command}
{stats}
"""

# for OSG
CONDOR_TEMPLATE = """#!/bin/bash
executable = /home/ershockley/cax/osg_scripts/run_xenon.sh
universe = vanilla
Error = /xenon/ershockley/cax/$(pax_version)/$(name)/$(zip_name).log
Output  = /xenon/ershockley/cax/$(pax_version)/$(name)/$(zip_name).log
Log     = /xenon/ershockley/cax/$(pax_version)/$(name)/joblogs/$(zip_name).joblog

Requirements = ((OpSysAndVer == "CentOS6" || OpSysAndVer == "RedHat6" || OpSysAndVer == "SL6") && (GLIDEIN_ResourceName =!= "NPX")) && (GLIDEIN_ResourceName =!= "BU_ATLAS_Tier2")
request_cpus = $(ncpus)
transfer_input_files = /home/ershockley/user_cert, {json_file}
transfer_output_files = ""
+WANT_RCC_ciconnect = True
when_to_transfer_output = ON_EXIT
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
transfer_executable = True
# periodic_release =  ((NumJobStarts < 5) && ((CurrentTime - EnteredCurrentStatus) > 600))
arguments = $(name) $(input_file) $(host) $(pax_version) $(pax_hash) $(out_location) $(ncpus) $(disable_updates) $(json_file)
queue 1
"""

def processing_script(args={}):
    host = get_hostname()
    if host not in ('midway-login1', 'tegner-login-1', 'login'):
        raise ValueError

    midway = (host == 'midway-login1')
    tegner = (host == 'tegner-login1')
    ci = (host == 'login')

    if midway or tegner:

        default_args = dict(host=host,
                            use='cax',
                            number=333,
                            ncpus=4 if midway else 1,
                            pax_version=(('v%s' % pax.__version__) if midway else 'head'),
                            #                        partition='sandyb' if midway else 'main',
                            partition='xenon1t' if midway else 'main',
                            #                        partition='kicp' if midway else 'main',
                            base='/project/lgrandi/xenon1t' if midway else '/cfs/klemming/projects/xenon/xenon1t',
                            account='pi-lgrandi' if midway else 'xenon',
                            anaconda='/project/lgrandi/anaconda3/bin' if midway else '/afs/pdc.kth.se/projects/xenon/software/Anaconda3/bin',
                            extra='#SBATCH --mem-per-cpu=2000\n#SBATCH --qos=xenon1t' if midway else '#SBATCH -t 72:00:00',
                            #                        extra='#SBATCH --mem-per-cpu=2000\n#SBATCH --qos=xenon1t-kicp' if midway else '#SBATCH -t 72:00:00',
                            #                        extra2='source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh' if midway else '',
                            #                        extra='#SBATCH --mem-per-cpu=2000' if midway else '#SBATCH -t 72:00:00',
                            stats='sacct -j $SLURM_JOB_ID --format="JobID,NodeList,Elapsed,AllocCPUS,CPUTime,MaxRSS"' if midway else ''
                            )

        for key, value in default_args.items():
            if key not in args:
                args[key] = value

        # Evaluate {variables} within strings in the arguments.
        args = {k:v.format(**args) if isinstance(v, str) else v for k,v in args.items()}

        os.makedirs(args['base']+"/"+args['use']+("/%s"%str(args['number']))+"_"+args['pax_version'], exist_ok=True)

        # Script parts common to all sites
        script_template = """#!/bin/bash
#SBATCH --job-name={use}_{number}_{pax_version}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={ncpus}
#SBATCH --mem-per-cpu=2000
#SBATCH --time=0:60:00

#SBATCH --output={base}/{use}/{number}_{pax_version}/{number}_{pax_version}_%J.log
#SBATCH --error={base}/{use}/{number}_{pax_version}/{number}_{pax_version}_%J.log
#SBATCH --account={account}
#SBATCH --partition={partition}

{extra}

export PATH={anaconda}:$PATH
export JOB_WORKING_DIR={base}/{use}/{number}_{pax_version}
mkdir -p ${{JOB_WORKING_DIR}}
cd ${{JOB_WORKING_DIR}}

rm -f pax_event_class*
source activate pax_{pax_version}


HOSTNAME={host}
{command}


{stats}
""".format(**args)

    elif ci:
        default_args = dict(host=host,
                            use='cax',
                            number=28,
                            ncpus=1,
                            pax_version=('v%s' % pax.__version__),
                            partition='n/a',
                            pax_hash='n/a',
                            base='/xenon/xenon1t/raw',
                            account='n/a',
                            anaconda='/stash2/project/@xenon1t/anaconda3/bin',
                            extra='n/a',
                            stats='',
                            )

        fill_args(args, default_args)
        # Evaluate {variables} within strings in the arguments.
        args = {k: v.format(**args) if isinstance(v, str) else v for k, v in args.items()}
        # os.makedirs(args['base']+"/"+args['use']+("/%d"%args['number'])+"_"+args['pax_version'], exist_ok=True)
        script_template = CONDOR_TEMPLATE.format(json_file=args['json_file'])  # .format(**args)

    else:
        raise NotImplementedError("Host %s processing not implemented",
                                  host)

    return script_template



def get_base_dir(category, host):
    destination_config = get_config(host)

    # Determine where data should be copied to
    return destination_config['dir_%s' % category]


def get_raw_base_dir(host=get_hostname()):
    return get_base_dir('raw', host)


def get_processing_base_dir(host=get_hostname()):
    return get_base_dir('processed', host)


def get_processing_dir(host, version):
    return os.path.join(get_processing_base_dir(host),
                        'pax_%s' % version)


def get_minitrees_base_dir(host=get_hostname()):
    return get_base_dir('minitrees', host)


def get_minitrees_dir(host, version):
    return os.path.join(get_minitrees_base_dir(host),
                        'pax_%s' % version)


def adjust_permission_base_dir(base_dir, destination):
    """Set ownership and permissons for basic folder of processed data (pax_vX)"""

    if destination=="tegner-login-1":
      #Change group and set permissions for PDC Stockholm
      user_group = DATA_USER_PDC + ":" + DATA_GROUP_PDC
      
      subprocess.Popen( ["chown", "-R", user_group, base_dir],
                        stdout=subprocess.PIPE )
                             

      subprocess.Popen( ["setfacl", "-R", "-M", "/cfs/klemming/projects/xenon/misc/basic", base_dir],
                        stdout=subprocess.PIPE )


def get_adler32( fname ):
  """Calcualte an Adler32 checksum in python
     Used for cross checks with Rucio
  """
  
  BLOCKSIZE=256*1024*1024
  asum = 1
  with open(fname, "rb") as f:
    while True:
      data = f.read(BLOCKSIZE)
      if not data:
        break
      asum = adler32(data, asum)
      if asum < 0:
        asum += 2**32

  return hex(asum)[2:10].zfill(8).lower()

#Rucio stuff:
def set_rucio_rse( rucio_rse):
    """Set the rucio rse information manually
    """
    global RUCIO_RSE
    RUCIO_RSE = rucio_rse

def set_rucio_scope( rucio_scope):
    """Set the rucio scope information manually
    """
    global RUCIO_SCOPE
    RUCIO_SCOPE = rucio_scope

def set_rucio_upload( rucio_upload ):
    global RUCIO_UPLOAD
    RUCIO_UPLOAD = rucio_upload

def set_rucio_campaign( rucio_campaign ):
    global RUCIO_CAMPAIGN
    RUCIO_CAMPAIGN = rucio_campaign

def set_rucio_rules( config_rule ):
    """Set the according config file to define the rules for transfer"""
    global RUCIO_RULE
    RUCIO_RULE = config_rule

def load_dag_config():
    # User-specified config file
    c = get_config()
    dag_config = os.path.join(os.path.dirname(__file__, c['dag_config']))

    logging.debug('Loading dag config file %s' % dag_config)

    return json.loads(open(dag_config, 'r').read())
