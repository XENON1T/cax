import checksumdir
import socket
import os

from urllib.request import Request, urlopen, HTTPSHandler, build_opener
import tempfile
import os
from urllib.error import URLError, HTTPError
import http.client

from html.parser import HTMLParser

from paramiko import SSHClient, util
from scp import SCPClient
import config

def copy(f1, f2,
         server,
         username):
    util.log_to_file('ssh.log')
    ssh = SSHClient()
    ssh.load_system_host_keys()
    
    ssh.connect(server,
                username=username)


    # SCPCLient takes a paramiko transport as its only argument
    scp = SCPClient(ssh.get_transport())

    scp.put(f1, f2,
            recursive=True)

    scp.close()

def upload():
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # For each TPC run, check if should be uploaded
    for doc in collection.find({'detector' : 'tpc'}):
        # For this run, where can we upload to?
        for remote_host in config.upload_options():
            # Grab the configuration of this host
            remote_config = config.get_config(remote_host)

            there = False # Is data remote?
            datum_here = None # Information about data here

            # Iterate over data locations to know status
            for datum in doc['data']:
                # Is host known?
                if 'host' not in datum:
                    continue

                # If the location refers to here
                if datum['host'] == config.get_hostname():
                    # Was data transferred here?
                    if datum['status'] == 'transferred':
                        # If so, store info on it.
                        datum_here = datum
                elif datum['host'] == remote_host:  # This the remote host?
                    # Is the data already there (checked or not)?
                    if datum['status'] == 'transferred' or datum['status'] == 'verifying':
                        there  = True
            
            if datum_here and not there:
                datum_there = {'type' : datum_here['type'],
                               'host' : remote_host,
                               'status' : 'transferring',
                               'location' : remote_config['directory'],
                               'checksum' : None}
                collection.update({'_id': doc['_id']},
                                  {'$push': {'data': datum_there}})
                print('copying', remote_host)
                copy(datum_here['location'],
                     remote_config['directory'],
                     remote_config['hostname'],
                     remote_config['username'])

                datum_there['status'] = 'verifying'
                collection.update({'_id': doc['_id'],
                                   'data.host' : datum_there['host']},
                                  {'$set': {'data.$' : datum_there}})
    
    

if __name__ == "__main__":
    
    upload()
