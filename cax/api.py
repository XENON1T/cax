import requests
import logging
from json import dumps
from bson import json_util
from cax import config

class api():
    def __init__(self):
        # Runs DB Query Parameters
        self.api_url = config.API_URL
        self.get_params = {
            "username": config.api_user(),
            "api_key": config.api_key(),
            "detector": config.DETECTOR
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
            
            ret = json_util.loads(requests.get(self.api_url,
                                               params = params).text)
            
        else:
            ret = json_util.loads(requests.get(self.next_run).text)

        # Keep track of the next run so we can iterate. 
        if ret is not None:
            self.next_run = ret['meta']['next']
            if len(ret['objects'])==0:
                return None
            
            return json_util.loads(ret['objects'][0]['doc'])

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
        parameters=dumps(parameters)
        ret = requests.put(url, data=parameters,
                           headers=self.data_set_headers)
        
    def remove_location(self, uuid, parameters):    
        # Removes a data location from the list        
        parameters['status'] = "remove"
        self.add_location(uuid, parameters)
        
    def update_location(self, uuid, remove_parameters, add_parameters):
        # Removes location from the list then adds a new one
        self.remove_location(uuid, remove_parameters)
        self.add_location(uuid, add_parameters)
