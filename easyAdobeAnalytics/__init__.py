import requests
import pandas as pd
import json
import time
import copy
from easyAdobeAnalytics.clean_adobe_response import json_to_df

class easyAdobeAnalytics:
    def __init__(self,client_id:str,client_secret:str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = self.authentication()
    
    def authentication(self) -> str:
        url = "https://ims-na1.adobelogin.com/ims/token/v3"

        payload = f'grant_type=client_credentials&client_id={self.client_id}&client_secret={self.client_secret}&scope=openid%2CAdobeID%2Cadditional_info.projectedProductContext'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    def queue_report(self,report_description,global_company_id):
        # Set report queue endpoint
        report_queue_url = "https://api5.omniture.com/admin/1.4/rest/?method=Report.Queue"

        # Set request headers
        headers = {
            "Authorization": "Bearer {}".format(self.access_token),
            "Content-Type": "application/json",
            "x-api-key": self.client_id,
            'x-proxy-global-company-id':global_company_id,
        }

        # Set report request payload
        report_request = {
            "reportDescription": report_description
        }

        # Make POST request to report queue endpoint
        response = requests.post(
            report_queue_url, headers=headers, json=report_request)

        # Parse response and extract report ID
        if response.status_code == 200:
            report_data = json.loads(response.text)
            report_id = report_data["reportID"]
            return report_id
        else:
            raise ValueError(
                "Report queueing failed. Error: {}".format(response.text))

    def get_report(self,report_id):
        # Set report get endpoint
        report_get_url = "https://api.omniture.com/admin/1.4/rest/?method=Report.Get"

        # Set request headers
        headers = {
            "Authorization": "Bearer {}".format(self.access_token),
            "Content-Type": "application/json",
            "x-api-key": self.client_id,
        }
        payload = { "reportID": report_id}

        # Make GET request to report get endpoint
        response = requests.post(report_get_url, headers=headers, json=payload)

        # Poll report status until it is ready
        while response.status_code == 400:
            time.sleep(5)
            response = requests.post(report_get_url, headers=headers, json=payload)

        # Parse response and extract report data
        if response.status_code == 200:
            report_data = json.loads(response.text)
            return report_data
        else:
            raise ValueError(
                "Report retrieval failed. Error: {}".format(response.text))
        
    def clean_classification_response(self,report_data:dict) -> dict:
        if 'elements' in report_data['report']:
            for element in report_data['report']['elements']:
                if element.get('classification',None):
                    element['name'] = element['name'] + ' ' + element['classification']

        return report_data

    def query_all_reports(self,reports,company_id,elements) -> pd.DataFrame:
        for i, report_description in enumerate(reports):
            report_id = self.queue_report(report_description, company_id)
            report_data = self.get_report(report_id)
            report_data = self.clean_classification_response(report_data)
            report_data = json_to_df(report_data,elements)
            if i == 0:
                dataframes = report_data
            else:
                dataframes = pd.concat([dataframes,report_data])
        return dataframes
    
    def get_all_rsid(self):
        url = "https://api.omniture.com/admin/1.4/rest/?method=Company.GetReportSuites"
        headers = {
            "Authorization": "Bearer {}".format(self.access_token),
            "Content-Type": "application/json",
            "x-api-key": self.client_id,
        }
        response = requests.get(url, headers=headers)
        return response.json()
    
    @staticmethod
    def generate_report_description(elements:list,metrics:list,segments:list,rsid:str,date_from:str,date_to:str,date_granularity:str,query_segments_individually:bool=True):
        report_description = {
            "reportSuiteID": rsid,
            "dateFrom": date_from,
            "dateTo": date_to,
            "dateGranularity": date_granularity,
            "elements": [],
            "metrics": [],
            "segments": [],
            "locale": "en_US"}
        for element in elements:
            if element.find('.') != -1:
                report_description['elements'].append(
                    {
                        "id": element.split('.')[0],
                        "classification": element.split('.')[1],
                        "top": "50000"
                    }
                )
            else:
                report_description['elements'].append(
                    {
                        "id": element,
                        "top": "50000"
                    }
                )
        for metric in metrics:
            report_description['metrics'].append(
                {
                    "id": metric
                }
            )

        if segments:
            if query_segments_individually:
                reports = []
                for segment in segments:
                    report_description['segments'] = [{
                            "id": segment
                        }]
                    reports.append(copy.deepcopy(report_description))
            else:
                report_description['segments'] = []
                for segment in segments:
                    report_description['segments'].append(
                        {
                            "id": segment
                        }
                    )
                reports = [report_description]

            return reports
                
        else:
            return [report_description]
        
def query_and_retrieve(client_id,client_secret,elements,metrics,segments,rsid,date_from,date_to,date_granularity,company_id,query_segments_individually=True) -> pd.DataFrame:
    a = easyAdobeAnalytics(client_id,client_secret)
    reports = easyAdobeAnalytics.generate_report_description(elements,metrics,segments,rsid,date_from,date_to,date_granularity,query_segments_individually)
    
    report_data = a.query_all_reports(reports,company_id,elements)
    return report_data