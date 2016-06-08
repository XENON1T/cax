"""Configuration routines
"""

import json
import logging
import os
import socket

import pymongo

# global variable to store the specified .json config file
CAX_CONFIGURE = ''
DATABASE_LOG = True

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
    return socket.gethostname().split('.')[0]


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
                            if get_config(to['host'])['receive'] == 'method']

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
        uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017/run'
        uri = uri % os.environ.get('MONGO_PASSWORD')
        c = pymongo.MongoClient(uri,
                                replicaSet='runs',
                                read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
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


def processing_script(host):
    # Script parts common to all sites
    script_template = """#!/bin/bash
#SBATCH --job-name={name}_{pax_version}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={ncpus}
#SBATCH --mem-per-cpu=2000
#SBATCH --mail-type=ALL
"""
    # Midway-specific script options
    if host == "midway-login1":
        script_template += """
#SBATCH --output=/project/lgrandi/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --error=/project/lgrandi/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --account=pi-lgrandi
#SBATCH --qos=xenon1t
#SBATCH --partition=xenon1t
#SBATCH --mail-user=pdeperio@astro.columbia.edu

export PATH=/project/lgrandi/anaconda3/bin:$PATH

export PROCESSING_DIR=/project/lgrandi/xenon1t/processing/{name}_{pax_version}
        """
    elif host == "tegner-login-1": # Stockolm-specific script options
        script_template = """
#SBATCH --output=/cfs/klemming/projects/xenon/common/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --error=/cfs/klemming/projects/xenon/common/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --account=xenon
#SBATCH --partition=main
#SBATCH -t 72:00:00
#SBATCH --mail-user=Boris.Bauermeister@fysik.su.se

source /afs/pdc.kth.se/home/b/bobau/load_4.8.4.sh

export PROCESSING_DIR=/cfs/klemming/projects/xenon/xenon1t/processing/{name}_{pax_version}
# WARNING: Boris should check this directory and the one above for log/error output ^        
#     multiple instances of pax should be run in separate directories to avoid clash of libraries     
        """
    else:
        raise NotImplementedError("Host %s processing not implemented",
                                  host)

    # Script parts common to all sites
    script_template += """
mkdir -p ${{PROCESSING_DIR}} {out_location}
cd ${{PROCESSING_DIR}}
rm -f pax_event_class*

source activate pax_{pax_version}

echo time cax-process {name} {in_location} {host} {pax_version} {pax_hash} {out_location} {ncpus}
time cax-process {name} {in_location} {host} {pax_version} {pax_hash} {out_location} {ncpus}

mv ${{PROCESSING_DIR}}/../logs/{name}_*.log ${{PROCESSING_DIR}}/.
"""
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
