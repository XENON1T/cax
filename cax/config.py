"""Configuration routines
"""

import json
import logging
import os

import pymongo

# global variable to store the specified .json config file
CAX_CONFIGURE = ''
DATABASE_LOG = True
HOST = os.environ.get("HOSTNAME")

PAX_DEPLOY_DIRS = {
    'midway-login1' : '/project/lgrandi/deployHQ/pax',
    'tegner-login-1': '/afs/pdc.kth.se/projects/xenon/software/pax'
}


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


def get_config(hostname=get_hostname()):
    """Returns the cax configuration for a particular hostname
    NB this currently reloads the cax.json file every time it is called!!
    """
    for doc in load():
        if doc['name'] == hostname:
            return doc
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

    return options


def get_task_list():
    try:
        options = get_config(get_hostname())['task_list']
    except LookupError as e:
        logging.debug("task_list not specified, running all tasks")
        return []

    return options


def mongo_collection(collection_name='runs_new'):
    # For the event builder to communicate with the gateway, we need to use the DAQ network address
    # Otherwise, use the internet to find the runs database
    if get_hostname().startswith('eb'):
        c = pymongo.MongoClient(
            'mongodb://eb:%s@gw:27017/run' % os.environ.get('MONGO_PASSWORD'))
    else:
        uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
        uri = uri % os.environ.get('MONGO_PASSWORD')
        c = pymongo.MongoClient(uri,
                                replicaSet='runs')
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


def processing_script(args={}):
    host = get_hostname()
    if host not in ('midway-login1', 'tegner-login-1'):
        raise ValueError

    midway = (host == 'midway-login1')
    default_args = dict(host=host,
                        use='cax',
                        number=333,
                        ncpus=1,
                        pax_version='head',
                        partition='xenon1t' if midway else 'main',
                        base='/project/lgrandi/xenon1t' if midway else '/cfs/klemming/projects/xenon/xenon1t',
                        account='pi-lgrandi' if midway else 'xenon',
                        email='pdeperio@astro.columbia.edu' if midway else 'Boris.Bauermeister@fysik.su.se',
                        extra='' if midway else '',
                        anaconda='/project/lgrandi/anaconda3/bin' if midway else '/afs/pdc.kth.se/projects/xenon/software/Anaconda3r5/bin')

#    default_args['command'] = 'cax --once --run %d'

    for key, value in default_args.items():
        if key not in args:
            args[key] = value

    # Evaluate {variables} within strings in the arguments.
    args = {k:v.format(**args) if isinstance(v, str) else v for k,v in args.items()}

    #args['command'] = args['command'].format(**default_args)

    # Script parts common to all sites
    script_template = """#!/bin/bash
#SBATCH --job-name={use}_{number}_{pax_version}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={ncpus}
#SBATCH --mem-per-cpu=2000
#SBATCH --mail-type=ALL

#SBATCH --output={base}/{use}/logs/{number}_{pax_version}_%J.log
#SBATCH --error={base}/{use}/logs/{number}_{pax_version}_%J.log
#SBATCH --account={account}
#SBATCH --partition={partition}
#SBATCH --mail-user={email}

{extra}

export PATH={anaconda}:$PATH

export JOB_WORKING_DIR={base}/{use}/{number}_{pax_version}

mkdir -p ${{JOB_WORKING_DIR}}
cd ${{JOB_WORKING_DIR}}
rm -f pax_event_class*

#source activate pax_{pax_version}
source activate test_env

HOSTNAME={host} {command}

""".format(**args)
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
