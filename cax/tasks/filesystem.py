"""File system operations through cax

This allows you to move or remove files while still notifying the run database,
but also checks for strays.
"""

import logging
import os
import shutil
import subprocess

from cax import config
from cax.task import Task

class SetPermission(Task):
    """Set the correct permissions at the PDC in Stockholm"""

    def __init__(self):

        self.raw_data = {"tegner-login-1": "/cfs/klemming/projects/xenon/xenon1t/raw/",
                         "midway-login1": "/project2/lgrandi/xenon1t/raw/"}
        
        self.proc_data = {"tegner-login-1": "/cfs/klemming/projects/xenon/xenon1t/processed/",
                          "midway-login1": "/project/lgrandi/xenon1t/processed/"}
        
        self.chown_user = {"tegner-login-1": "bobau",
                           "midway-login1": "mklinton"}

        self.chown_group = {"tegner-login-1": "xenon-users",
                            "midway-login1": "xenon1t-admins"}

        self.chmod = {"tegner-login-1": '750',
                      "midway-login1": '755'}

        Task.__init__(self)
        self.hostname_config = config.get_config(config.get_hostname())
        self.hostname = config.get_hostname()

    def each_run(self):
        """Set ownership and permissons for files/folders"""
        for data_doc in self.run_doc['data']:
            # Is not local, skip
            if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
                continue


            #extract path:
            f_path = data_doc['location']
            f_type = data_doc['type']

            #apply changes according to processed/raw and analysis facility
            if f_type == 'processed':
              logging.info('Change ownership and permission for %s', f_path)
              logging.info('Change to username %s and group %s', self.chown_user[self.hostname], self.chown_group[self.hostname])
              logging.info('Set permission: %s', self.chmod[self.hostname] )
              logging.info('Set ownership and permissions at %s', config.get_hostname() )
              if config.get_hostname() == "midway-login1":
                subprocess.call(['chmod', self.chmod[self.hostname], f_path])
                subprocess.call(['chown', str(self.chown_user[self.hostname]+":"+self.chown_group[self.hostname]), f_path])
              elif config.get_hostname() == "tegner-login-1":
                subprocess.call(['chmod', self.chmod[self.hostname], f_path])
                subprocess.call(['chown', str(self.chown_user[self.hostname]+":"+self.chown_group[self.hostname]), f_path])
                subprocess.call(['setfacl', '-R', '-M', '/cfs/klemming/projects/xenon/misc/basic_file', f_path])
              else:
                logging.info('Analysis facility does not match')
            elif f_type == 'raw':
              logging.info('Change ownership and permission for %s', f_path)
              logging.info('Change to username %s and group %s', self.chown_user[self.hostname], self.chown_group[self.hostname])
              logging.info('Set permission: %s', self.chmod[self.hostname] )
              logging.info('Set ownership and permissions at %s', config.get_hostname() )
              if config.get_hostname() == "midway-login1":
                subprocess.call(['chmod', '-R', self.chmod[self.hostname], f_path])
                subprocess.call(['chown', '-R', str(self.chown_user[self.hostname]+":"+self.chown_group[self.hostname]), f_path])
              elif config.get_hostname() == "tegner-login-1":
                subprocess.call(['chmod', self.chmod[self.hostname], f_path])
                subprocess.call(['chown', str(self.chown_user[self.hostname]+":"+self.chown_group[self.hostname]), f_path])
                subprocess.call(['setfacl', '-R', '-M', '/cfs/klemming/projects/xenon/misc/basic', f_path])
              else:
                logging.info('Analysis facility does not match')

            else:
              logging.info("Nothing to change: Ownership/Permission")


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
            try:
              shutil.move(self.input, self.output)
            except Exception as e:
              print( e )

            ## Notify run database
            if config.DATABASE_LOG is True:
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {
                                           'data.$.location': self.output}})
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


class AddSize(Task):

    """ Evaluate the file size of a raw data and check if
        the number of files in the raw data directory match
        with the number of events recorded
    """
    def __init__(self):
        Task.__init__(self)

    def each_run(self):

        # Check of data info
        if 'data' not in self.run_doc:
           self.log.debug("Data not found Name: %s " %(self.run_doc['name']) )
           return

        # Check of Trigger info
        if 'trigger' not in self.run_doc:
           self.log.debug("Trigger not found Name: %s " %(self.run_doc['name']) )
           return

        run_number = self.run_doc['number']
        run_name = self.run_doc['name']
        trigger = self.run_doc['trigger']
        reader = self.run_doc['reader']

        if 'events_built' not in trigger:
	   self.log.debug("Events not build in Name: %s " %(self.run_doc['name']) )
	   return

        evn_per_zip = 0
        
        if 'trigger_config_override' not in reader['ini']:
            evn_per_zip = 1000
        elif 'Zip' not in reader['ini']['trigger_config_override']:
            evn_per_zip = 1000
        else:
            evn_per_zip = reader['ini']['trigger_config_override']['Zip']['events_per_file']
        
        self.log.debug("Event per zip: %d" % (evn_per_zip))


        for data_doc in self.run_doc['data']:

            _host = data_doc['host']
            _location = data_doc['location']
            _type = data_doc['type']
            _status = data_doc['status']

            try:
                if _type ==  "raw" and _status == "transferred":
                    self.log.debug("host: %s  location: %s  type: %s" %(_host,_location,_type ) )

                    nevents = trigger['events_built']
                    ents = int(nevents)/evn_per_zip
                    self.log.debug("Number of Events: %d" % (nevents))
                    self.log.debug("Number of Zip Files: %i" % (int(ents)+1))

                    # Check if the number of files match with the number of events
                    completeness = False
                    raw_size = 0
                    nfiles = len(os.listdir(_location))-4
                    
                    
                    if 'raw_size_byte' not in self.run_doc:
                        
                        # The Muon Veto data have one file less respect to the TPC run
                        if ( (nfiles) == (int(ents)) or (nfiles) == (int(ents)+1) ) :
                            self.log.debug("nevnt: %i  nfile: %i" %(nfiles,int(ents) ) )
                            self.log.debug("Run Name: %s  Number: %d  is on %s: %s" %
                                           (run_name,run_number, _host, _location))
                            completeness = True
                        else:
                            self.log.debug("!!! Corrupted !!! Expected %d files found %d  Name: %s " %((int(ents)+1), nfiles, run_name) )
                            completeness = False
                         
                        # If the raw files are complete calculate the size summing the size of
                        # each file contained in the directory of the raw data
                        if (completeness):

                            for i in os.listdir(_location):
                                #self.log.info(_location+"/"+i)
                                rfile=_location+"/"+i
                                byt= os.stat(rfile)
                                raw_size += byt.st_size
                         
                            self.log.debug("Raw size: %d Byte  %.2f GByte nEvents: %d Size per evnt: %.1f" %
                                             (raw_size,raw_size*1e-9, nevents,float(raw_size/nevents) ))
                         
                            self.collection.update( {'_id' : self.run_doc['_id'] },
                                                      {'$set': {'raw_size_byte' : raw_size } } )
                    else:
                        self.log.debug("Size Raw data: %.2f GB" % (float(self.run_doc['raw_size_byte']/1.e9)))


                #This adds the size of the processed files if are present in the local disk.
                if ( _type ==  "processed" and _status == "transferred" ):
                   if 'size' not in data_doc:
                       if os.path.isfile(_location):
                           byt= os.stat(_location)
                           self.log.debug("Location: %s Size: %i Byte (%.2f GB)"
                                          % (_location, byt.st_size, byt.st_size/1024/1024/1024 ) )
                           self.collection.update({'_id' : self.run_doc['_id'],
                                               'data': {'$elemMatch': data_doc}},
                                              {'$set': {'data.$.size': byt.st_size } } )
                   else:
                       if os.path.isfile(_location):
                           byt= os.stat(_location)
                           self.log.debug("Location: %s  Size: %.2f GB"
                                          % (_location,byt.st_size/1024/1024/1024 ) )

            except FileNotFoundError:
                if run_number == 0:
                    self.log.debug("Run name: %s  Not on %s "%(run_name,_location))
                else:
                    self.log.debug("Run: %i  Name: %s  Not on %s "%(run_number,run_name,_location))
                continue
   

class RemoveTSMEntry(Task):
    """Remove a single file or directory

    This notifies the run database.
    """

    def __init__(self, location):
        # Save filesnames to use
        self.location = location
        print("delete from database: ", self.location)
        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
        # For each data location, see if this filename in it
        #print(self.run_doc)
        cnt = 0
        for data_doc in self.run_doc['data']:
            # Is not local, skip
            if data_doc['host'] == "tsm-server":
                print("found: ", data_doc)
                cnt+=1

        for data_doc in self.run_doc['data']:

            if 'host' not in data_doc or data_doc['host'] != "tsm-server":
                continue

            if data_doc['location'] != self.location:
                continue

            # Notify run database
            if config.DATABASE_LOG is True:
              print("Delete this: ", data_doc)
              res = self.collection.update({'_id': self.run_doc['_id']},
                                           {'$pull': {'data': data_doc}}
                                           )
              for key, value in res.items():
                print( " * " + str(key) + ": " + str(value) )
            else:
              print("This should be deleted:")
              print(data_doc)
            break

        print("There are {a} entries in the runDB with \"tsm-server\" for the same run number or name".format(a=str(cnt)))

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

    def __init__(self, node__, status__):
        # Save filesnames to use
        self.node = node__
        self.status = status__

        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
        # print(self.run_doc['data'])

        # For each data location, see if this filename in it
        for data_doc in self.run_doc['data']:
            #Show entries only for requested host
            if self.node == data_doc['host'] and self.status == data_doc[
                'status']:
                status_db = data_doc["status"]
                location_db = data_doc['location']
                logging.info("Ask for status %s at node %s: %s", self.node,
                             status_db, location_db)

                # TODO
                # Add a memberfunction to change the status manually:

class RemoveRucioEntry(Task):
    """Remove a single file or directory

    This notifies the run database.
    """

    def __init__(self, location, status):
        # Save filesnames to use
        self.location = location
        self.status = status
        print("delete from database: ", self.location)
        Task.__init__(self)

    def each_run(self):
        # For each data location, see if this filename in it
        #print(self.run_doc)
        cnt = 0
        for data_doc in self.run_doc['data']:
            # Is not local, skip
            
            if self.status == None:
              if data_doc['host'] == "rucio-catalogue":
                print("found: ", data_doc)
                cnt+=1
            else:
              if data_doc['host'] == "rucio-catalogue" and data_doc['status'] == self.status:
                print("found: ", data_doc)
                cnt+=1  
        
        for data_doc in self.run_doc['data']:
            
            if 'host' not in data_doc or data_doc['host'] != "rucio-catalogue":
                continue

            if data_doc['location'] != self.location:
                continue
            
            if self.status != None and data_doc['status'] != self.status:
                continue
            print("----------")
            print(data_doc)
            # Notify run database
            if config.DATABASE_LOG is True:
              print("Delete this: ", data_doc)
              res = self.collection.update({'_id': self.run_doc['_id']},
                                           {'$pull': {'data': data_doc}}
                                           )
              for key, value in res.items():
                print( " * " + str(key) + ": " + str(value) )
            else:
              print("This should be deleted:")
              print(data_doc)
            break

class RuciaxTest(Task):
    """Remove a single file or directory

    This notifies the run database.
    """

    def __init__(self, mode, location):
        # Save filesnames to use
        self.location = location
        self.mode = mode

        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
        # For each data location, see if this filename in it
        #print(self.run_doc)
        cnt = 0

        #for data_doc in self.run_doc['data']:
            ## Is not local, skip
            #if data_doc['host'] == "rucio-catalogue":
                #print("found: ", data_doc)
                #cnt+=1

        cnt_rucio = 0
        locArra = []
        for data_doc in self.run_doc['data']:

          if 'host' not in data_doc or data_doc['host'] != "rucio-catalogue":
            continue

            #if data_doc['location'] != self.location:
                #continue
          if all (k in data_doc for k in ("rucio-catalogue")):
            print( "They're there!", k )

          if data_doc['host'] == "rucio-catalogue":
            cnt_rucio += 1
            locArra.append( data_doc['location'] )

        #if das != None and cnt_rucio > 1:
          print(locArra, cnt_rucio, data_doc['status'])
          print(" ")

        #print("There are {a} entries in the runDB with \"rucio-catalogue\" for the same run number or name".format(a=str(cnt)))
