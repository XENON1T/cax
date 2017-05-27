import requests
import logging
from json import dumps
from bson import json_util
from cax import config

class api():
    def __init__(self):

        logging.getLogger('requests').setLevel(logging.ERROR)

        if ( config.API_URL is None or config.api_user() is None
             or config.api_key() is None ):
            raise NameError("API connectivity options not found")
        
        # Runs DB Query Parameters
        self.api_url = config.API_URL
        self.api_schema = "https://xenon1t-daq.lngs.infn.it"
        self.get_params = {
            "username": config.api_user(),
            "api_key": config.api_key(),
            "detector": 'all'
        }
        self.next_run = "init"

        # Runs DB writing parameters
        self.data_set_headers = {
            "content-type": "application/json",
            "Authorization": "ApiKey "+config.api_user()+":"+config.api_key()
        }
        
        self.logging = logging.getLogger(self.__class__.__name__)

    def get_next_run(self, query):
        ret = None
        if self.next_run == None:
            return ret
        if self.next_run is "init":
            # Prepare query parameters
            params = self.get_params
            if 'detector' in params and params['detector'] == 'all':
                params.pop('detector')
            for key in query.keys():
                params[key] = query[key]

            params['limit']=1
            params['offset']=0
            
            api_try = 1
            while api_try <= 3:
                try:
                    db_request = requests.get(self.api_url, params = params).text
                    break
                except:
                    time.sleep(5)
                    api_try += 1
                if api_try == 3:
                    print("Error: API call to database failed!")
                    return None

            ret = json_util.loads(db_request)
            
        else:
            ret = json_util.loads(requests.get(self.api_schema + self.next_run).text)

        # Keep track of the next run so we can iterate. 
        if ret is not None:
            self.next_run = ret['meta']['next']
            if len(ret['objects'])==0:
                return None
            
            return ret['objects'][0]['doc']

        return None
    
    def add_location(self, uuid, parameters):
        # Adds a new data location to the list

        # Parameters must contain certain keys.
        required = ["host", "location", "checksum", "status", "type"]
        if not all(key in parameters for key in required):
            raise NameError("attempt to update location without required keys")

        url = self.api_url + str(uuid) + "/"

        # BSON/JSON confusion. Get rid of date field.
        if 'creation_time' in parameters:
            parameters.pop('creation_time')
        pars=dumps(parameters)
        ret = requests.put(url, data=pars,
                           headers=self.data_set_headers)

        # This checks to make sure the location was added/removed/updated
        # GET request
        params = self.get_params
        doc = json_util.loads(requests.get(self.api_url+str(uuid),
                                           params=params).text)['doc']

        # We removed the location
        if parameters['status'] == 'remove':

            if 'data' not in doc:
                return True
            for entry in doc['data']:
                
                if self.verify_site(entry, parameters):
                    print(entry)
                    print(parameters)
                    raise RuntimeError("Failed to update run doc 1")
        else:
            if 'data' not in doc:
                raise RuntimeError("Failed to update run doc 2")
            for entry in doc['data']:
                if self.verify_site(entry, parameters):
                    return True
            raise RuntimeError("Failed to update run doc 3")
                
    def remove_location(self, uuid, parameters):    
        # Removes a data location from the list        
        parameters['status'] = "remove"
        self.add_location(uuid, parameters)
        
    def update_location(self, uuid, remove_parameters, add_parameters):
        # Removes location from the list then adds a new one
        self.remove_location(uuid, remove_parameters)
        self.add_location(uuid, add_parameters)

    def verify_site(self, sitea, siteb):
        # We assume two data entries are identical if the host, type,
        # and path are the same
        return ( (sitea['host'] == siteb['host']) and
                 (sitea['type'] == siteb['type']) and
                 (sitea['location'] == siteb['location']))

    def get_all_runs(self, query, limit):
        # return list of rundocs for all runs satisfying query
        collection = []
        counter = 0
        while self.next_run is not None and counter < limit:
            collection.append(self.get_next_run(query))
            counter += 1

        return collection