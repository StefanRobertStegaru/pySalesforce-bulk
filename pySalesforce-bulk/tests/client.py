DEFAULT_API_VERSION = '58.0'
import requests

from .login import salesforce_login
class SalesforceClient:
    def __init__(self,username,password,domain,client_id,client_secret,version=DEFAULT_API_VERSION):
        login=salesforce_login(username=username,password=password,domain=domain,client_id=client_id,client_secret=client_secret)
        self.headers=login[0]
        self.instance_url=login[1]

        self.root_url=f"{self.instance_url}/services/data/v{version}"
    

    def call_api(self,method,url_endpoint,body,params=None):


        url=f"{self.root_url}{url_endpoint}"
        call_api_request=requests.request(method=method,url=url,headers=self.headers,json=body,params=params)
       
        return call_api_request