import time
from .client import SalesforceClient
from .job import SalesforceJob
import pandas


class Salesforce:
    def __init__(self, username :str, password:str, domain:str, client_id:str, client_secret:str) -> None:
        self.client = SalesforceClient(username=username, password=password,
                                       domain=domain, client_id=client_id, client_secret=client_secret)

    def get_data(self, query:str, include_deletes:bool=True)-> pandas.DataFrame:
        if include_deletes:
            operation='query'
        else:
            operation='queryAll'
        job=SalesforceJob(client=self.client,type='query')

        job.create(operation=operation,query=query)

        counter_stop = False
        state=''
        while counter_stop == False:
            state=job.state
            if  state== "JobComplete":
                counter_stop = True
            elif state in ('Failed', 'Aborted'):
                counter_stop = True
                job.delete()
            else:
                time.sleep(30)

        if state == "JobComplete":
            data = job.result
            job.delete()
            return data
        else:
            raise Exception(
                f"Job with job_id {job.job_id} failed with state {state}")

    def upsert_data(self,object:str,external_field_name :str,pandas_df : pandas.DataFrame):
        pass
