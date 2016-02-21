import checksumdir
import socket
import os

from urllib.request import Request, urlopen, HTTPSHandler, build_opener
import tempfile
import os
from urllib.error import URLError, HTTPError
import http.client

from html.parser import HTMLParser

BASE_URL = 'https://tbn18.nikhef.nl/dpm/nikhef.nl/home/xenon.biggrid.nl/'

class ExtractGRIDFilesnames(HTMLParser):
    ignore_metalink = False
    wait = True
    filenames = []

    def handle_starttag(self, tag, attrs):
        if tag == 'thead':
            self.wait = False
        if tag == 'a': 
            if not self.ignore_metalink and not self.wait: 
                for name, value in attrs:
                    if name == 'href':
                        self.filenames.append(value)
        if tag == 'td':
            for name, value in attrs:
                if name == 'class' and value == 'metalink':
                    self.ignore_metalink = True
    def handle_endtag(self, tag):
        self.ignore_metalink = False

    def handle_data(self, data):
        pass#print("Encountered some data  :", data)

    def get_filenames(self):
        return self.filenames


class GRIDAuthHandler(HTTPSHandler):
    """Used for accessing GRID data and handling authentication"""
    def __init__(self, key, cert):
        HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return http.client.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


def download_url(url, f,
                 key = '/home/xedaq/.globus/userkey.nopass.pem',
                 cert = '/home/xedaq/.globus/usercert.pem'):

    opener = build_opener(GRIDAuthHandler(key,
                                          cert))

    response = opener.open(url)
    block_sz = 8192
    while True:
        buffer = response.read(block_sz)
        if not buffer:
            break
        f.write(buffer)
    f.seek(0)


def download_run(run_name, directory_local):
    url_directory = os.path.join(BASE_URL, run_name)
    parser = ExtractGRIDFilesnames()

    f = tempfile.NamedTemporaryFile()
    download_url(url_directory, f)
    parser.feed(open(f.name).read())
    f.close()

    if not os.path.isdir(directory_local):
        os.makedirs(directory_local)

    for filename in parser.get_filenames():
        url_file = os.path.join(url_directory,
                                filename)
        filename_local = os.path.join(directory_local,
                                      filename)

        f = open(filename_local, 'wb')
        download_url(url_file, f)
        f.close()

        
def hash_run(run_name):
    with tempfile.TemporaryDirectory() as directory:
        download_run(run_name, directory)
        hash_value = checksumdir.dirhash(directory, 'sha512')
        print(hash_value)
        
#download_run('junji', 'temp')
#hash_run('junji')
