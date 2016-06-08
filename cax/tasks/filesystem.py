"""File system operations through cax

This allows you to move or remove files while still notifying the run database,
but also checks for strays.
"""

import os
import shutil
import logging

from cax import config
from cax.task import Task
import os


class RenameSingle(Task):
    """Rename a file

    This renames a file or folder then updates the run database to reflect it.
    This is an unsafe operation since it does not perform a new checksum.
    """

    def __init__(self, input, output):
        # Save filesnames to use
        self.input = os.path.abspath(input)
        self.output = os.path.abspath(output)

        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
        # For each data location, see if this filename in it
        for data_doc in self.run_doc['data']:
            # Is not local, skip
            if 'host' not in data_doc or \
                            data_doc['host'] != config.get_hostname():
                continue

            if data_doc['location'] != self.input:
                continue

            self.log.info("Moving %s to %s" % (self.input,
                                               self.output))
            # Perform renaming
            os.renames(self.input,
                      self.output)

            # Notify run database
            if config.DATABASE_LOG is True:
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.location': self.output}})
            break


class RemoveSingle(Task):
    """Remove a single file or directory

    This notifies the run database.
    """

    def __init__(self, location):
        # Save filesnames to use
        self.location = os.path.abspath(location)

        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
        # For each data location, see if this filename in it
        for data_doc in self.run_doc['data']:
            # Is not local, skip
            if 'host' not in data_doc or \
                            data_doc['host'] != config.get_hostname():
                continue

            if data_doc['location'] != self.location:
                continue

            # Notify run database
            if config.DATABASE_LOG is True:
                self.collection.update({'_id': self.run_doc['_id']},
                                       {'$pull': {'data': data_doc}})

            # Perform operation
            self.log.info("Removing %s" % (self.location))
            if os.path.isdir(data_doc['location']):
                shutil.rmtree(data_doc['location'])
            else:
                os.remove(self.location)

            break

class FindStrays(Task):
    """Remove a single file or directory

    This notifies the run database.
    """

    locations = []

    def each_location(self, data_doc):
        if data_doc['host'] == config.get_hostname():
            self.locations.append(data_doc['location'])

    def check(self, directory):
        if directory is None:
            return

        for root, dirs, files in os.walk(directory, topdown=False):
            if root in self.locations:
                continue
            for name in files:
                if os.path.join(root, name) not in self.locations:
                    print(os.path.join(root, name))
            for dir in dirs:
                if os.path.join(root, dir) not in self.locations:
                    if root != config.get_processing_base_dir():
                        print(os.path.join(root, name)) 

    def shutdown(self):
       self.check(config.get_raw_base_dir())
       self.check(config.get_processing_base_dir())
       
class StatusSingle(Task):
    """Status of a single file or directory

    This notifies the run database.
    """

    def __init__(self, node__, status__, set__, file__):
        # Save filesnames to use
        self.node = node__
        self.status = status__
        self.set = set__
        self.file = file__

        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
        #print(self.run_doc['data'])
        
        # For each data location, see if this filename in it
        for data_doc in self.run_doc['data']:
            ## Is not local, skip
            if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
                continue
                
            if self.node == data_doc['host'] and self.status == data_doc['status'] and self.file == None and self.set == None:
              #cax-status --disable_database_update --node tegner-login-1 --status verifying
              status_db = data_doc["status"]
              location_db = data_doc['location']
              logging.info("Ask for status %s at node %s: %s", self.node, status_db, location_db)
            
            if self.node == data_doc['host'] and self.status == data_doc['status'] and self.set != None and self.file == "all":
              #example: cax-status --file all --disable_database_update --node tegner-login-1 --status verifying --set processed
              if config.DATABASE_LOG is True:
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.status': self.set}})
              logging.info('File/Data: %s is set from status %s to %s', data_doc['location'], data_doc['status'], self.set)
            
            if self.node == data_doc['host'] and self.status == data_doc['status'] and self.set != None and self.file == data_doc['location']:
              #example: cax-status --file /cfs/klemming/projects/xenon/xenon1t/processed/pax_v4.10.0/160530_0505.root --disable_database_update --node tegner-login-1 --status verifying --set processed
              if config.DATABASE_LOG is True:
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.status': self.set}})
              logging.info('File/Data: %s is set from status %s to %s', data_doc['location'], data_doc['status'], self.set)
              
            if self.node == data_doc['host'] and self.status == None and self.file == data_doc['location'] and self.set == None:
              logging.info('Status of %s is: ', data_doc['location'], data_doc["status"] )  
              #example: cax-status --file /cfs/klemming/projects/xenon/xenon1t/processed/pax_v4.10.0/160530_0505.root --disable_database_update --node #tegner-login-1
