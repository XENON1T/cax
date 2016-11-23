# Rucio Admin Guide for Xenon Collaboration

NOTE: This is only for users with privileged accounts. Others cannot execute these command

The Rucio admin tools (`rucio-admin` in the commandline) is the collection of commands to configure and manage the Rucio instance. 

## Installing Rucio Admin

The admin tools have the same dependencies as the client. After the dependencies have been installed you can install the client through:

* Login into the system where the Rucio client will be installed
* Execute `pip install --user rucio`
* Check if `~/.local/rucio` is present
* Append the `PATH` to allow access to the rucio binary: `export PATH=~/.local/bin:$PATH`
* Set `RUCIO_HOME`: `export RUCIO_HOME=~/.local/rucio`
* Set `RUCIO_ACCOUNT`: `export RUCIO_ACCOUNT=username`
* Modify your `.bashrc` or equivalent to add the Rucio client to your `PATH`, and set `RUCIO_HOME` and `RUCIO_ACCOUNT` whenever you log in
* Get the Xenon1T Rucio configuration file: 
```
cd ~/.local/rucio/etc
wget http://stash.osgconnect.net/@xenon1t/public/rucio/rucio.cfg
```
* Check if Rucio is setup properly by running `rucio whoami`. It should output something similar to:
```
$ rucio whoami
Enter PEM pass phrase:
status  : ACTIVE
account : sthapa
account_type : USER
created_at : 2016-07-01T17:35:47
suspended_at : None
updated_at : 2016-07-01T17:35:47
deleted_at : None
email   : ssthapa@uchicago.edu
```

## Frequently Used Commands

* Creating scope
```
rucio-admin scope add --help
usage: rucio-admin scope add [-h] --account ACCOUNT --scope SCOPE
optional arguments:
  -h, --help        show this help message and exit
  --account ACCOUNT  Account name
  --scope SCOPE     Scope name
```
* Creating an account
    * Adding account
      `rucio-admin account add <username>`
    * Map account to an identity:
        ```
        rucio-admin identity add -h
        usage: rucio-admin identity add [-h] --account ACCOUNT --type
                                        {X509,GSS,USERPASS} --id IDENTITY --email  EMAIL
        optional arguments:
          -h, --help            show this help message and exit


          --account ACCOUNT     Account name
          --type {X509,GSS,USERPASS}
                                Authentication type [X509|GSS|USERPASS]
          --id IDENTITY         Identity
          --email EMAIL         Email address associated with the identity
        ```
    * Create user scope
        ```
        rucio-admin scope add --help
        usage: rucio-admin scope add [-h] --account ACCOUNT --scope SCOPE
        optional arguments:
          -h, --help        show this help message and exit
          --account ACCOUNT  Account name
          --scope SCOPE     Scope name
        ```
* Registering an RSE
    * To add the RSE
        `rucio-admin rse add <rse>`
    * Set attributes for the RSE
        ```
        rucio-admin rse set-attribute --help
        usage: rucio-admin rse set-attribute [-h] --rse RSE --key KEY --value VALUE
        ```
    * Configure gridftp for an existing endpoint. From python command line:
        ```
        from rucio.core.rse import add_protocol
        proto = {‘hostname’: endpoint.host.name,
                 ‘scheme’: ‘gsiftp’,
                 ‘port’: port,
                 ‘prefix’: /path/to/storage,
                 ‘impl’: ‘rucio.rse.protocols.gfal.Default’,
                 ‘extended_attributes’: None,
                 ‘domains’: {‘lan’: {‘read’: 1,
                             ‘write’: 1,
                             ‘delete’: 1},
                 ‘wan’: {‘read’: 1,
                         ‘write’: 1,
                         ‘delete’: 1}}}
        add_protocol(<rse>, parameter=proto)
        ```
    * Set distances between the RSEs
        ```
        from rucio.core.rse import list_rses
        from rucio.core.distance import add_distance
        rses = list_rses()
        for src in rses:
            for dst in rses:
                add_distance(src_rse_id=src['id'], dest_rse_id=dst['id'], ranking=1, agis_distance=1)
        ```