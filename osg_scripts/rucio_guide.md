# Rucio User Guide for Xenon Collaboration

Xenon1T will have large amount of data stored in individual files across multiple remote storage sites. Managing these files, including their location, relationships between individual files and sets of files, etc., and transfer between storage sites, as well as to and from jobs, requires a data management system. The data management system has to enable the users to identify and operate on any arbitrary set of files. 

For Xenon1T, we have chosen to use the ATLAS data management system Rucio. It is pure python framework that uses a SQL database as the backend to storage the required information about the data and the relationship between individual data sets. For Rucio, files are the smallest operational unit.

# Rucio-specific Terms

Rucio has some specific terms that will how up in the documentation:

## DID

Dataset identifiers (DIDs) are the core objects in Rucio. They may be files, datasets or containers. Every DID has a unique combination of a scope (defined below) and a name. The Rucio clients always display this as `scope:name`. This implies that an identifier, once used, can never be reused to refer to anything else at all, not even if the data it referred to has been deleted from the system.

When a command requires a DID argument it must be the full DID (`scope:name`) or simply the scope plus a wildcard (`scope:*`). Rucio commands asking for a DID and can accept files, datasets or containers. 

## Scope

Scopes in Rucio and are a way of partitioning the filesystem namespace. On the filesystem level, the scope represents the top-level directory inside the Rucio storage area. If the data is stored at `/xenon/rucio/` in the storage site's filesystem and scope is `bar`, the individual file `foo.bar` (DID is `bar:foo.bar`) will be stored at `/xenon/rucio/bar/XX/YY/foo.bar`, where `XX` and `YY` are parts of the `md5` checksum of `foo.bar`. Scopes starting with `user.` or `group.`, are special in Rucio. They will have a file system structure of `/xenon/rucio/user/joe/XX/YY/foo.bar` for a scope of `user.jdoe` and `/xenon/rucio/group/DMAnalysis/XX/YY/foo.bar` for a scope of `group.DMAnalysis`.


Scopes also have certain sets of read, write, and creation permissions depending on the users. By default, normal user accounts will have read access to all scopes and write access only to their own scope. Privileged (or admin) accounts will have write access to multiple scopes, e.g. the account `production` might use scopes such as `mc16`, `data16`, etc. that reference the official datasets of the collaboration. Users have one default scope `user.<username>`, for example for user `jdoe` this is `user.jdoe`. Associated with this scope a user can generaate whatever DID he or she may desire, e.g. user jdoe can create `user.jdoe:mytest` for a dataset or `user.jdoe:foo.root` for a file. Vanilla users cannot create additional scopes. Only users with administrator priviliges can create scopes and associate them with an account. 

## RSE

Rucio Storage Element (RSE) is an abstraction for storage end-points, eg. `LNGS_DAQDISK` might be the RSE where data from the DAQ system are copied for distribution throughout the Rucio network. At NIKHEF, you might have an RSE called `NIKHEF_DATADISK` which receives raw data for processing.  The terms "site" and "RSE" are used interchangeably. The DIDs are stored on RSEs. The RSEs can be tagged with different tags which can be useful when the number of RSEs grows large.

### Deterministic vs. Non-deterministic RSE

The next question is: do you generate the physical path of the file programmatically or not? Rucio has a concept of a deterministic RSE, which means that a simple function applied to the `scope:name` can give the full path on the RSE.  The primary reason for this is to divide the namespace on disk for file system performance. If not the RSE has to be defined as non-deterministic and the full replica paths should be provided to the the `list_replicas` method.

## Dataset

A dataset is a named set of files. It has the format of any other DID, namely `scope:name`. A dataset is not reflected in the filesystem of the storage sites. It only exists inside the database.

## Container

A container a named set of datasets or, recursively, containers. It has the format of any other DID, namely `scope:name`. A container is not reflected in the filesystem of the storage sites. It only exists inside the database.

## (Replication) Rules 

The replication rules (aka rules) define how a DID should be or is replicated among the RSEs. A rule is associated to an account, a DID and an RSE. When a rule is set on a DID, and a particular RSE, and the DID is not present at the site Rucio will initiate (an) FTS transfer(s) to the site from an RSE for the file(s) associated with the particular DID. If the DID is present, the rule will be ignored. If a replication rule is present, the DID cannot be deleted from that RSE. Otherwise, the DID will be deleted in case the RSE does not have sufficient storage available. 

# Getting an Account

To get an account within rucion you need your grid certificate and to have your grid certifcate added to the Xenon VO. For instrustions in how to get a grid certificate and add yourself to the Xenon VO, see [the instructions on the Xenon1t wiki](https://xecluster.lngs.infn.it/dokuwiki/doku.php?id=xenon:xenon1t:sim:grid). Once you have your grid certificate all set up, you need to email the Rucio admin (?????) with the following information:

* Preferred username for the account

* The DN and email for the x509 certificate registered in the xenon.biggrid.nl VO: `$ openssl x509 -in .globus/usercert.pem -noout -subject -email`

# Rucio Software

The Rucio software is split into two pieces: client and admin. 

## Rucio Client

The Rucio client (simply `rucio` in the command line) allows users all the necessary tasks to manage their own data. This includes, uploading data, downloading data, creating replication rules, listen data locations, etc.

### Installing Client

The Rucio client is installed using pip and can be installed locally in your home directory or (not recommended) globally in case you have root priviliges. Installing the Rucio client requires the following dependencies (these require root privileges on the host for installation or access to the OSG OASIS cvmfs respository):

* GCC compiler
* pip
* Gfal2 python (gfal2-python rpm)  - This is available from the EPEL repository. 
* CA certificates and updated CRLs  in /etc/grid-security/certificates.
* Libffi devel packages (libffi-devel rpm) - This is in the standard Scientific Linux repositories
* Kerberos devel packages (kerberos-devel) - This is in the standard Scientific Linux repositories
* Python devel packages (python-devel) - This is in the standard Scientific Linux repositories
* Openssl devel packages (openssl-devel) - This is in the standard Scientific Linux repositories

After the dependencies are satisfied the user can install the client without root priviliges. NOTE: These instructions are meant for normal user accounts that do not have root priviliges on the machine.

* Login into the system where the Rucio client will be installed
* Execute `pip install --user rucio-clients-xenon1t`
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

### Rucio Client through cvmfs

Details to follow

### Frequently Used Commands

* Change which user account to use:
`rucio -a <Rucio username> <Rucio command>`
* User identity: 
`rucio whoami`
* Create a dataset
`rucio upload --rse MyRSE --scope user.jdoe user.jdoe:MyDatasetName <list of files>`
* Download a DID
`rucio download <DID>`
* Create an empty dataset
`rucio add-dataset user.jdoe:test.Dataset`
* Create an empty container
`rucio add-container user.jdoe:test.Container`
* Add (attach) DIDs to a dataset/container:
`rucio attach <dataset/container DID> <list_of_DIDs>`
* Delete (attach) DIDs from a dataset/container:
`rucio attach <dataset/container DID> <list_of_DIDs>`
* What are the active storage locations?
`rucio list-rses`
* Show all the file/dataset replicas
`rucio list-file-replicas <DID>`
`rucio list-dataset-replicas <pattern>`
* How to list the content of a DID
`rucio list-dids <scope>:*`, e.g. `rucio list-dids ‘user.jdoe:*’` will list all the datasets/containers belonging to a user jdoe in the scope `user.jdoe`
* List files in a dataset
`rucio list-files <dataset>`    
* List account usage
`rucio list-account-usage jdoe`
* Add a replication rule
`rucio add-rule <list of DIDs> <number of copies> <rse>`. Note this print a UUID of the rule to the screen
* Delete a replication rule
`rucio delete-rule rule_id`
* List rules concerning a DID
These can optionally be filtered by account, subscription or RULE_ID: 
`rucio list-rules`
* List the datasets at an RSE
`rucio list-datasets-rse <rse>`
* List usage at an RSE
`rucio list-rse-usage <rse>`


# Data Organization

For Xenon100, there are several ways how the data was organized. On NIKHEF: 

```
/dpm/nikhef.nl/home/xenon.biggrid.nl/
|
+-- archive
|   |
|   \-- data
|       |
|       \-- xenon100
|           |
|           \-- run_XX
|               |
|               \-- xe100_YYMMDD_HHMM
|                   |
|                   \--<files>
```

On the MWT2 Ceph pool:

```
/xenon/
|
+-- run_XX
|   |
|   \-- xe100_YYMMDD_HHMM
|       |
|       \--<files>
```

For Rucio, a combination of using scopes, containers, and datasets replaces the filesystem structure. Scopes are top level directories in a file system and are reflected in the filesystem, while the datasets and containers are similar to sub-directories yet are only reflected in the database. For the scopes `data`, `simulation`, and `user.jdoe`, the MWT2 Ceph pool would create a file system structure of:

```
/xenon/
|
+-- data
|   |
|   \-- XX
|       |
|       \-- YY
|           |
|           \-- foo.bar
|
+-- simulation
|   |
|   \-- ZZ
|       |
|       \-- AA
|           |
|           \-- sim_foo.bar
|
+-- user
|   |
|   \-- jdoe
|       |
|       \-- BB
|           |
|           \-- CC
|               |
|               \-- user_file_foo.bar
```


The subdirectories (ZZ/AA, BB/CC, etc.) for every scope in the above example are “deterministic”, i.e. can be calculated algorithmically.

# Endpoints


# Tutorial

Note: This tutorial assumes you are on `login.ci-connect.uchicago.edu`

Given that you have your grid certificate, are a memeber of the Xenon VO, and have an account in Rucio, we can now do some simple tasks with rucio. 

Before we can do anything with rucio, you have to get your grid certicate setup on the machine you are using. You should have a PKCS12 (it should look something like `user_certificate_and_key.UXXXX.p12`) file that you got when your grid certificate was approved. Copy the PKCS12 file to your home directory on the server you are using. Now create 

`mkdir $HOME/.globus`

then run

`openssl pkcs12 -in <PKCS12_filename> -clcerts -nokeys -out $HOME/.globus/usercert.pem`

`openssl pkcs12 -in <PKCS12_filename> -nocerts -out $HOME/.globus/userkey.pem`

and finally we need to change the permissions:

`chmod 0600 $HOME/.globus/usercert.pem`

`chmod 0400 $HOME/.globus/userkey.pem`

Now we can generate a VOMs or grid proxy. This is the active part of your grid certificate and will allow you to initiate transfers. 

First we need to setup the enviroment to get the grid tools and rucio. To use the grid tools for RHEL6 and derivatives rucio from the the Xenon OASIS CVMFS repository run:

`source /cvmfs/xenon.opensciencegrid.org/software/rucio-py26/setup_rucio_1_8_3.sh`

Once you are in the desired environment initiate the proxy run: 

`voms-proxy-init -voms xenon.biggrid.nl -valid 720:00`

Now lets try downloading a single file: 

`rucio download user.briedel:test_rucio.dummy`

This should download the file `test_rucio.dummy` into `$PWD/user.briedel`. If we now try to download a dataset with 

`rucio download user.briedel:tutorial`

It will put the files in that dataset into `$PWD/tutorial`. Now to upload a file, take a random file (you can generate a 100 MB test file using `dd if=/dev/urandom of=test_rucio.dummy bs=100M count=1`). Now run 

`rucio -v upload --rse UC_OSG_USERDISK --scope user.<your_Rucio_username> user.<your_Rucio_username>:tutorial_rucio /path/to/test/file` 

to upload a file. If the file has been uploaded successfully, run the command `rucio list-dids user.<your_Rucio_username>:tutorial_rucio` this should print something like:

```
+-------------------------------------------+--------------+
| SCOPE:NAME                                | [DID TYPE]   |
|-------------------------------------------+--------------|
| user.<your_Rucio_username>:tutorial_rucio | DATASET      |
+-------------------------------------------+--------------+
```

Now run `rucio list-file-replicas user.<your_Rucio_username>:tutorial`, this should prompt something like:

``` 
+----------------------------+------------------+------------+-----------+---------------------------------------------------------------------------------------------------------------------+
| SCOPE                      | NAME             | FILESIZE   | ADLER32   | RSE: REPLICA                                                                                                        |
|----------------------------+------------------+------------+-----------+---------------------------------------------------------------------------------------------------------------------|
| user.<your_Rucio_username> | test_rucio.dummy | 33.6 MB    | checksum  | UC_OSG_USERDISK: gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm/xenon/rucio/user/briedel/XX/YY/test_rucio.dummy |
+----------------------------+------------------+------------+-----------+---------------------------------------------------------------------------------------------------------------------+
```