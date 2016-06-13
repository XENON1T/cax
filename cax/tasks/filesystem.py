"""File system operations through cax

This allows you to move or remove files while still notifying the run database,
but also checks for strays.
"""

import os
import shutil
import logging
import subprocess

from cax import config
from cax.task import Task
import os

class SetPermission(Task):
    """Set the correct permissions"""
    
    def __init__(self):
      self.counter = 0
      self.chmod_file = '/cfs/klemming/projects/xenon/misc/basic_file'
      self.chmod_foder = '/cfs/klemming/projects/xenon/misc/basic'
            
      Task.__init__(self)
      self.destination_config = config.get_config( config.get_hostname() )

    def go(self):
      """Run the standard procedure to set ownership and permissons for files/folders"""
      self.ChangePermisson()
      self.SetCounter()
      self.ResetCounter(60) #Reset counter after 60 cycles: Permissions and ownership is changed every hour

    def ChangePermisson(self):
      set_rec = "-R"
      
      if self.counter == 0 and config.get_hostname() == "tegner-login-1":
        self.log.info("Set owner and group via chmod on host %s", config.get_hostname())  
        #self.log.info( ["chown", set_rec, "bobau:xenon-users", self.destination_config['dir_raw'] ] )
        #self.log.info( ["chown", set_rec, "bobau:xenon-users", self.destination_config['dir_processed'] ] )
        a_env = subprocess.Popen(["chown", set_rec, "bobau:xenon-users", self.destination_config['dir_raw'] ], stdout=subprocess.PIPE)
        b_env = subprocess.Popen(["chown", set_rec, "bobau:xenon-users", self.destination_config['dir_processed'] ], stdout=subprocess.PIPE)        
        
        self.log.info("Set permissions via setfacl on host %s", config.get_hostname())
        a_env = subprocess.Popen(["setfacl", set_rec, "-M", self.chmod_foder, self.destination_config['dir_raw'] ], stdout=subprocess.PIPE)
        b_env = subprocess.Popen(["setfacl", set_rec, "-M", self.chmod_foder, self.destination_config['dir_processed'] ], stdout=subprocess.PIPE)
        #self.log.info(["setfacl", set_rec, "-M", self.chmod_foder, self.destination_config['dir_raw'] ])
        #self.log.info(["setfacl", set_rec, "-M", self.chmod_foder, self.destination_config['dir_processed'] ])
      elif self.counter != 0:
        self.log.info("Nothing needs to be done")
      else:
        self.log.info("Host %s is not know for this permission setup", config.get_hostname())  
          
    def SetCounter(self):
      """Add +1 after each cycle"""
      self.counter += 1
        
    def ResetCounter(self, reset):
      """Reset the counter after a certain number of cycles"""
      if self.counter == reset:
        self.counter = 0


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

    def __init__(self, host__, status__, file__):
        # Save filesnames to use
        self.host = host__
        self.status = status__
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
                
            if self.host == data_doc['host'] and self.status == data_doc['status'] and self.file == None:
              #cax-status --disable_database_update --host tegner-login-1 --status verifying
              status_db = data_doc["status"]
              location_db = data_doc['location']
              logging.info("Ask for status %s at host %s: %s", self.host, status_db, location_db)
            
              
            if self.host == data_doc['host'] and self.status == None and self.file == data_doc['location']:
              logging.info('Status of %s is: ', data_doc['location'], data_doc["status"] )  
              #example: cax-status --file /cfs/klemming/projects/xenon/xenon1t/processed/pax_v4.10.0/160530_0505.root --disable_database_update --host #tegner-login-1
