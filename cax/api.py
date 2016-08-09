import requests
from json import dumps, loads

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

    def get_next_run(query):

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
            
            ret = requests.get(self.api_url, params = params)
            
        else:
            ret = requests.get(self.next_run)

        # Keep track of the next run so we can iterate. 
        if ret is not None:
            self.next_run = ret['next']
            return ret['doc']

        return None
    
    def add_location(uuid, parameters):
        # Adds a new data location to the list

        # Parameters must contain certain keys.
        required = ["host", "location", "checksum", "status", "type"]
        if not all(key in parameters for key in required):
            raise NameError("attempt to update location without required keys")

        url = self.api_url + uuid + "/"
        ret = requests.put(url, data=parameters,
                           headers=self.data_set_headers)
        
    def remove_location(uuid, parameters):    
        # Removes a data location from the list
        parameters['status'] = "remove"
        self.add_location(uuid, parameters)
        
    def update_location(uuid, remove_parameters, add_parameters):
        # Removes location from the list then adds a new one
        self.remove_location(uuid, remove_parameters)
        self.add_location(uuid, add_parameters)
