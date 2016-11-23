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

## Troubleshooting a Rucio instance



## Setting up a Rucio instance

### Dependency Installation - ADMIN ONLY

This is assuming RHEL7 system:

```
rpm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm
rpm -iUvh http://mirror.grid.uchicago.edu/pub/osg/3.3/osg-3.3-el7-release-latest.rpm
yum -y update
yum install osg-pki-tools
reboot
yum install -y python-pip
pip install --upgrade pip
yum -y install vim gcc python-devel krb5-devel
yum install -y memcached
systemctl enable memcached
yum install -y gfal2 gfal2-python
yum install mariadb-devel mariadb-server mariadb MySQL-python
chkconfig mariadb on
service mariadb restart
pip install rucio
```

### Creating SQL Database

```
mysql-->
create database rucio;
grant all privileges on rucio.* to 'rucio'@'localhost' identified by
'rucio';
```

### Creating Rucio Configuration

```
mkdir -p /var/log/rucio/trace
mkdir -p /opt/rucio/etc/web
cp /usr/rucio/tools /opt/rucio
cp /usr/rucio/etc/rucio.cfg.template /opt/rucio/etc/rucio.cfg
vi /opt/rucio/etc/rucio.cfg:
```

`rucio.cfg`:
    ```
    rucio_host      -> <HTTP_accessible_Rucio_hostname, e.g. https://rucio.mwt2.org:443>
    auth_host       -> <HTTP_accessible_Rucio_hostname, e.g. https://rucio.mwt2.org:443>
    carbon_server   -> <Graphite_hostname>
    carbon_port     -> <Graphite_port>
    ssl_key_file    -> <Location of host key, e.g. /etc/grid-security/hostkey.pem>
    ssl_cert_file   -> <Location of host cert, e.g. /etc/grid-security/hostcert.pem>
    voname          -> <VO_URL, e.g. xenon.biggrid.nl>
    username        -> <rucio_username>
    password        -> <pw>
    userpass_identity   -> <rucio_username>
    userpass_pwd   -> <pw>
    userpass_email -> <admin_email>
    X509_identity   -> <X509_host_cert_DN>
    x509_email      -> <X509_Host_cert_admin_email
    brokers         -> <activemq_host>
    port            -> <activemq_port_on_host>
    ftshosts        -> <FTS_server>
    default     -> <MySQL_URI>
    scheme      -> <Comma-separated_list_transfer_protocols>
    ```

Some config files are not in the above rucio installation, need to get from the git repo

```
git clone https://gitlab.cern.ch/rucio01/rucio.git ~/rucio
cp ~/rucio/etc/alembic.ini.template /opt/rucio/etc/alembic.ini
cp -r ~/rucio/lib/rucio/db/sqla /opt/rucio/lib/rucio/db # change line in alembic.ini to point to this dir
```

### Resetting/Setting Database Schema

```
cd /opt/rucio
tools/reset_database.py
```

### Setting up Apache/HTTP Server

```
yum -y install httpd mod_ssl mod_wsgi gridsite
systemctl enable httpd
rm -f /etc/httpd/conf.d/*
cp ~/rucio/etc/web/httpd-rucio-443-py26-slc6.conf.template /etc/httpd/conf.d/rucio.conf
cp ~/rucio/etc/web/aliases-py27.conf /opt/rucio/etc/
vi /opt/rucio/etc/aliases-py27.conf     # rucio python in /usr/lib/python2.7/site-packages/rucio
service httpd start


$ curl -k -X GET https://rucio.mwt2.org:443/ping
{"version": "1.5.0"}
```

### Daemon Configuration

In `/etc/supervisord.d/rucio.ini` append:

```
[program:rucio-conveyor-transfer-submitter]
command=/bin/rucio-conveyor-transfer-submitter
stdout_logfile=/var/log/rucio/conveyor-transfer-submitter.log


[program:rucio-conveyor-poller]
command=/bin/rucio-conveyor-poller
stdout_logfile=/var/log/rucio/conveyor-poller.log


[program:rucio-conveyor-finisher]
command=/bin/rucio-conveyor-finisher
stdout_logfile=/var/log/rucio/conveyor-finisher.log


[program:rucio-undertaker]
command=/bin/rucio-undertaker
stdout_logfile=/var/log/rucio/undertaker.log


[program:rucio-reaper]
command=/bin/rucio-reaper
stdout_logfile=/var/log/rucio/reaper.log


[program:rucio-necromancer]
command=/bin/rucio-necromancer
stdout_logfile=/var/log/rucio/necromancer.log


[program:rucio-abacus-account]
command=/bin/rucio-abacus-account
stdout_logfile=/var/log/rucio/abacus-account.log


[program:rucio-abacus-rse]
command=/bin/rucio-abacus-rse
stdout_logfile=/var/log/rucio/abacus-rse.log


[program:rucio-transmogrifier]
command=/bin/rucio-transmogrifier
stdout_logfile=/var/log/rucio/transmogrifier.log


[program:rucio-judge-evaluator]
command=/bin/rucio-judge-evaluator
stdout_logfile=/var/log/rucio/judge-evaluator.log


[program:rucio-judge-repairer]
command=/bin/rucio-judge-repairer
stdout_logfile=/var/log/rucio/judge-repairer.log


[program:rucio-conveyor-stager]
command=/bin/rucio-conveyor-stager
stdout_logfile=/var/log/rucio/conveyor-stager.log
```

### Client Configuration

Without a config the configuration looks something like:

```
$ cat /opt/rucio/etc/rucio.cfg
# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, <mario.lassnig@cern.ch>, 2012-2014
# - Thomas Beermann, <thomas.beermann@cern.ch>, 2012, 2015-2016
# - Cedric Serfon, <cedric.serfon@cern.ch>, 2013
```

Append to `/opt/rucio/etc/rucio.cfg`:

```
[common]
logdir = /var/log/rucio
loglevel = DEBUG
mailtemplatedir=/opt/rucio/etc/mail_templates


[client]
rucio_host = https://rucio.mwt2.org:443
auth_host = https://rucio.mwt2.org:443
auth_type = x509
ca_cert = ~/.rucio/ca.crt
client_cert = ~/.rucio/usercert.pem
client_key = ~/.rucio/userkey.pem
client_x509_proxy = ~/.rucio/x509up
request_retries = 3
```

### Create Host Certificate

```
mkdir /etc/grid-security
osg-cert-request -p 7737026282 -n "Lincoln Bryant" -e lincolnb@uchicago.edu -H rucio.mwt2.org -v ATLAS
osg-cert-retrieve 7508
mv hostcert.pem /etc/grid-security
mv hostkey.pem /etc/grid-security


[root@rucio ~]# crontab -l
10 0 * * * /bin/voms-proxy-init -cert /etc/grid-security/hostcert.pem -key /etc/grid-security/hostkey.pem -voms xenon.biggrid.nl -hours 48 -out /opt/rucio/etc/web/x509up -q > /dev/null 2>&1
15 */3 * * * /bin/fts-delegation-init -s https://fts.usatlas.bnl.gov:8446 --proxy /opt/rucio/etc/web/x509up -q > /dev/null 2>&1
```

### Install ActiveMQ

```
yum install java-1.8.0-openjdk
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.77-0.b03.el7_2.x86_64
wget http://apache.osuosl.org/activemq/5.13.2/apache-activemq-5.13.2-bin.tar.gz (plus gpg verification)
tar -xzf apache-activemq-5.13.2-bin.tar.gz
cd apache-activemq-5.13.2/
vi conf/jetty-realm.properties (change admin, user pws)
bin/activemq start


In web interface: (rucio.mwt2.org:8161, default ports)
add queues: Consumer.kronos.rucio.tracer
add topics: transfer.fts_monitoring_queue_state, rucio.tracer, rucio.fax\x
```
