
from .pandas_parser import PandasParser
import pandas


class SalesforceJob:
    def __init__(self, client, type):

        self.client = client

        if type not in ('query', 'ingest'):
            raise Exception(
                f'The following type: {self.type} is not supported, only type query or ingest is supported')
        else:
            self.type = type

        self.job_id = ''

        self.job_state = None

        self._job_result = None

        self.api_endpoint = f"/jobs/{self.type}"

        if self.type == 'query':
            self.job_result_endpoint = 'results'
        elif self.type == 'ingest':
            self.job_result_endpoint = {
                "successful": "successfulResults",
                "failed": "failedResults",
                "unprocessed": "unprocessedrecords"
            }

        self.pandas_parser = PandasParser()

    def create(self, operation, query=None, object=None, external_field_name=None):
        if operation not in ('query', 'queryAll', 'insert', 'delete', 'hardDelete', 'update', 'upsert'):
            raise Exception(
                f'The following operation: {operation} is not supported, check Salesforce Bulk API v2.0 documentation')
        else:
            body = {
                "operation": operation,
                "contentType": "CSV",
                "columnDelimiter": "COMMA",
                "lineEnding": "LF"
            }

        if self.type == 'query':
            if query is not None:
                body['query'] = query
            else:
                raise Exception('The query job type expects a query')

            if operation not in ('query', 'queryAll'):
                raise Exception(
                    f'The following operation: {operation} is not supported for query, check Salesforce Bulk API v2.0 documentation')

        elif self.type == 'ingest':
            if object is not None and external_field_name is not None:
                body['object'] = object
                body['external_field_name'] = external_field_name
            else:
                raise Exception(
                    'The ingest job type expects an object and an external_field_name')

            if operation not in ('insert', 'delete', 'hardDelete', 'update', 'upsert'):
                raise Exception(
                    f'The following operation: {operation} is not supported for ingest, check Salesforce Bulk API v2.0 documentation')

        else:
            raise Exception(
                f'The following type: {self.type} is not supported, only type query or ingest is supported')

        create_job_response = self.client.call_api(
            method='POST', url_endpoint=self.api_endpoint, body=self.body)

        if create_job_response.status_code == 200:
            create_job_response_data = create_job_response.json()
            self.job_id = create_job_response_data.get("id")
        else:
            raise Exception(
                f"Error code : {str(create_job_response.status_code)} in retrieved when job is created. For more details check Salesforce REST API Status Codes and Error Response")

    def _get_job_state(self):

        if self.job_id == '':
            raise Exception(f"You have to create the job first first")
        elif self.job_id is None:
            raise Exception(f"Something wrong happened, there is no job_id")
        else:
            get_job_info_response = self.client.call_api(
                method='GET', url_endpoint=f"{self.api_endpoint}/{self.job_id}")

            if get_job_info_response.status_code == 200:
                get_job_info_response_data = get_job_info_response.json()
                self.job_state = get_job_info_response_data.get("state")
            else:
                raise Exception(
                    f"Error code : {str(get_job_info_response.status_code)} in retrieved when job state is checked. For more details check Salesforce REST API Status Codes and Error Response")

    def _get_job_result(self):
        if self.job_id == '':
            raise Exception(f"You have to create the job first first")
        elif self.job_id is None:
            raise Exception(f"Something wrong happened, there is no job_id")

        if self.type == 'query':
            get_job_result_response = self.client.call_api(
                method='GET', url_endpoint=f"{self.api_endpoint}/{self.job_id}/{self.job_result_endpoint}")
            first_page = self.pandas_parser.get_pd_from_csv(
                get_job_result_response.text)

            sforce_locator = get_job_result_response.headers.get(
                'Sforce-Locator')

            if sforce_locator:

                stop_pagination = False
                pd_df_list = []
                locator_string = sforce_locator

                while stop_pagination == False:
                    params = {"locator": locator_string}
                    data_response_page = self.client.call_api(
                        method='GET', url_endpoint=f"{self.api_endpoint}/{self.job_id}/{self.job_result_endpoint}", params=params)
                    locator_string = data_response_page.headers.get(
                        'Sforce-Locator')
                    page = self.pandas_parser.get_pd_from_csv(
                        data_response_page.text)
                    pd_df_list.append(page)

                    if locator_string is None:
                        stop_pagination = True

                data = pandas.concat(pd_df_list)
            else:
                data = first_page

        elif self.type == 'ingest':
            successfull = self.client.call_api(
                method='GET', url_endpoint=f"{self.api_endpoint}/{self.job_id}/{self.job_result_endpoint.get('successful')}")
            failed = self.client.call_api(
                method='GET', url_endpoint=f"{self.api_endpoint}/{self.job_id}/{self.job_result_endpoint.get('failed')}")
            unprocessed = self.client.call_api(
                method='GET', url_endpoint=f"{self.api_endpoint}/{self.job_id}/{self.job_result_endpoint.get('unprocessed')}")
            
            data={}
            if successfull:
                successfull_pd=self.pandas_parser.get_pd_from_csv(successfull.text)
                data['successful']=successfull_pd
            if failed:
                failed_pd=self.pandas_parser.get_pd_from_csv(failed.text)
                data['failed']=failed_pd
            if unprocessed:
                unprocessed_pd=self.pandas_parser.get_pd_from_csv(unprocessed.text)
                data['unprocessed']=unprocessed_pd
        
        return data

    def delete(self):

        if self.state in ("UploadComplete", "JobComplete", "Aborted", "Failed"):
            delete_request=self.client.call_api(
                    method='DELETE', url_endpoint=f"{self.api_endpoint}/{self.job_id}")

            if delete_request.status_code == 204:
                return f"Job with job_id {self.job_id} was successfully deleted"
            else:
                raise Exception(
                    f"Deleting job with id {self.job_id} failed with status code:{str(delete_request.status_code)}")
        else:
            raise Exception(
                    f"Only jobs with state UploadComplete, JobComplete, Aborted, or Failed can be deleted. This job is in state {self.state}")
        
    def set_state(self,state):
        body={"state":state}
        set_state_request=self.client.call_api(method='PATCH', url_endpoint=f"{self.api_endpoint}/{self.job_id}",body=body)
    
        return set_state_request

    @property
    def state(self):
        if self._job_state is None:
            self._get_job_state()
        return self._job_state

    @property
    def result(self):
        if self._job_result is None:
            self._get_job_result()
        return self._job_result

    @state.setter
    def state(self, value):
        self._job_state = value

    @result.setter
    def result(self, value):
        self._job_result = value
