===============================
Copying All XENON1T
===============================

.. image:: http://img.shields.io/badge/gitter-XENON1T/computing-blue.svg 
    :target: https://gitter.im/XENON1T/computing

Copying XENON1T data around

* Free software: ISC license
* Documentation: https://cax.readthedocs.org.


Overview
--------

This program is a daemonized data-management tool used within the XENON1T collaboration.  The idea is that a 'cax' daemon is run at each computing site within the collaboration.  Each daemon is responsible for common data management tasks on both raw and processed data, such as:

* Distribution of data (push and pull) via SCP
* Checksumming data
* Processing raw data into processed data
* Reporting information about data to a central MongoDB instance.

The configuration of each site is done via a JSON file called `cax.json` within this repository `cax.json <https://github.com/XENON1T/cax/blob/master/cax/cax.json>`_.  Each site has it's own section that allows each site to know where it can push or pull data from and also any information about processing.  

All information about the data and its copies is stored within MongoDB.  This is a central MongoDB that requires authentication.  If it is not already setup for you, you have to specify an environmental variable MONGODB_PASSWORD to use cax.  If you are a member of XENON1T, you can see what information is known about each run by using the run database website <https://xenon1t-daq.lngs.infn.it/runs>.

Changes to this repository are deployed to each site using DeployHQ <https://www.deployhq.com>.  This service has a web hook for this responsitory that deploys then installs new versions of `cax` each time the master branch is changed.  This copying happens via SCP.

The requirements to run `cax` at a new site are:

1. SSH-key access from the sites listed in `cax.json <https://github.com/XENON1T/cax/blob/master/cax/cax.json>`_ to the new site.
2. A directory specified in `cax.json <https://github.com/XENON1T/cax/blob/master/cax/cax.json>`_ where the data is located.  This needs to be able to be read, written once, and deleted.
3. Ability to run a daemon or process in the background permanently.  This is frequently done in a `screen` session.

Installation
------------

Cax is written in Python; we recommend the scientific python distribution `Anaconda <https://store.continuum.io/cshop/anaconda/>`_. 

In case your system already has Anaconda installed (with paramiko and pymongo libraries), e.g. analysis facilities or anywhere pax/cax is already running, you can skip to the next section.

To install this in Linux do::

  wget http://repo.continuum.io/archive/Anaconda3-2.4.0-Linux-x86_64.sh  # Linux
  bash Anaconda3-2.4.0-Linux-x86_64.sh  # Say 'yes' to appending to .bashrc and specify the installation directory

You can setup the environemnt by doing::

  conda create -n cax python=3.4 paramiko pymongo

Setting Up the Anaconda Libraries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add to your path environment the Anaconda bin location, e.g.::

  export PATH=~/anaconda3/bin:$PATH  # If installed in default location, otherwise replace with the 
                                     # path you specified above or the path of the central installation 

Installing cax
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Checkout and install::

  git clone https://github.com/XENON1T/cax.git
  source activate <environment_name>  # In case paramiko & pymongo are setup in <environment_name>, e.g. pax_head
  cd cax
  python setup.py install --prefix ${PWD} # For installing your own local copy. For common use within the Anaconda distribution, remove "--prefix"

Note, the last command will complain about the specified prefix path not being included in PYTHONPATH. So add it and try again::

  export PYTHONPATH=${PATH_IT_COMPLAINED_ABOUT}:${PYTHONPATH}
  python setup.py install --prefix ${PWD}

Using cax
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First ensure cax/cax.json is the list of sites that you wish to work with.

Then you can run a single instance with::

  bin/cax --once  # or just 'cax --once' if commonly installed
  
or continuously running daemon:: 

  bin/cax  # or just cax if commonly installed
  
This will perform the upload and downloads that are specified in cax.json and update the Runs DB accordingly. 

  https://xenon1t-daq.lngs.infn.it/runs
  
For checksumming, cax must be run on the storage server whose IP must be whitelisted by LNGS (contact ctunnell@nikhef.nl for this).

Processing is currently implemented for only Midway and Stockholm.

Customizing cax
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Customizing the behavior of cax is currently done in `cax/cax.json <https://github.com/XENON1T/cax/blob/master/cax/cax.json>`_, however you should be very careful when modifying this since the head version by default is being used by various sites to handle the XENON1T data flow and processing.

You can feed in a custom cax.json into cax via::

  cax --config <path to custom cax.json>

For development and testing, in addition to the options already in cax.json, you may specify subset of tasks you wish to run, e.g.::

  "task_list": ["ProcessBatchQueue", "AddChecksum"]

corresponding to the tasks in `cax/main.py <https://github.com/XENON1T/cax/blob/master/cax/main.py#L51>`_.

You may also specify a subset of datasets to operate on with, e.g.::

  "dataset_list": ["160315_1432", "160315_1514"]
  
Beware that in most tasks are commands that modify the Runs DB live, so for development you should comment out these commands prior to testing. A development flag is currently being developed to make this easier.


Additional cax tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the following there is a small overview given about a set of tools which is derived from cax

cax-status
^^^^^^^^^^

This tool allows to check the data base entries for a certain status (e.g. error, transfered, processed...)
  
  cax-status --host <host> --status <status>
  
  <host>: tegner-login-1, midway-login1,... (see cax.json)
  <status>: error, transferred, transferring, processed, verifying
  
  cax-status --host <host> --file <file>
  
  <host>: tegner-login-1, midway-login1,... (see cax.json)
  <file>: The full path to a root file or data set.

cax-process
^^^^^^^^^^^

Using cax to reprocess XENON1T data sets with pax in the background and register them in the run data base

  cax-process:
  
  --in-location              Specify the location of the raw data
  --name NAME                Specify the name
  --host HOST                Specify the host
  --pax-version              Specify the name
  --pax-hash                 Specify the pax_hash (Not necessary to manual reprocessing)
  --out-location             Specify the location for storing the root file
  --cpus NCPUS               Specify the number of cpus
  --disable_database_update  Disable the update function the run data base
  
  
Credits
---------

Please see the AUTHORS.rst file for information about contributors.

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
