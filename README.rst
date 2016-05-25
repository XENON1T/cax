===============================
Copying All XENON1T
===============================

.. image:: https://img.shields.io/pypi/v/cax.svg
        :target: https://pypi.python.org/pypi/cax

.. image:: https://img.shields.io/travis/tunnell/cax.svg
        :target: https://travis-ci.org/tunnell/cax

.. image:: https://readthedocs.org/projects/cax/badge/?version=latest
        :target: https://readthedocs.org/projects/cax/?badge=latest
        :alt: Documentation Status


Copying XENON1T data around

* Free software: ISC license
* Documentation: https://cax.readthedocs.org.

Installation
------------

Cax is written in Python; we recommend the scientific python distribution `Anaconda <https://store.continuum.io/cshop/anaconda/>`_. 

In case your system already has Anaconda installed (with paramiko and pymongo libraries), e.g. analysis facilities or anywhere pax/cax is already running, you can skip to the next section.

To install this in Linux do::

  wget http://repo.continuum.io/archive/Anaconda3-2.4.0-Linux-x86_64.sh  # Linux
  bash Anaconda3-2.4.0-Linux-x86_64.sh  # Say 'yes' to appending to .bashrc and specify the installation directory

You will also need the following libraries:

``conda install paramiko pymongo``

  
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


Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

Write description in cax.json
run db -> json ->

untriggered, raw, processed
