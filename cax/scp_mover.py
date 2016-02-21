import checksumdir
import socket
import os

from urllib.request import Request, urlopen, HTTPSHandler, build_opener
import tempfile
import os
from urllib.error import URLError, HTTPError
import http.client

from html.parser import HTMLParser

from paramiko import SSHClient
from scp import SCPClient
import config

BASE_URL = '/data/xenon/xenon1t'


def upload_run(run_name, directory_local):    
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('login.nikhef.nl', username='ctunnell')

    # SCPCLient takes a paramiko transport as its only argument
    scp = SCPClient(ssh.get_transport())

    scp.put('/home/xedaq/test.txt', '/user/ctunnell/test.txt')

    scp.close()

def main():
    print(config.upload_options('scp'))


if __name__ == "__main__":
    main()
