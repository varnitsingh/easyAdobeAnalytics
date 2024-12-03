# easyAdobeAnalytics

This is an attempt at a usable python library to query report data from Adobe Analytics 1.4 API.

# How it works
* For authentication, you need to retrieve an access token from Adobe using client id and client secret.
* First step is generating a json structure required by Adobe for querying data.
* Depending upon if you need segments to be queried individually or not, generate the required number of report descriptions.
* Next, we submit these reports to Adobe Analytics to ready the reports for us.
* Once the report is queried, Adobe returns us a `report_id` which we can use to track it's status.
* If report is not ready yet, we keep checking until it's ready and data is available to be consumed.
* Once the report is ready (depending upon the size of data), we get the actual report data using the `report_id`.
* Finally we concatenate all the report data returned to create a single dataframe.

# How to use

Install the package using:
```
pip install git+https://github.com/varnitsingh/easyAdobeAnalytics.git
```
All the functionality is behind the `query_and_retrieve` function in the package. Define all the variables required and pass it on. Leave the variable as empty list for `elements,metrics,segments` in case you don't wish to provide one for a report.

Example:
```
from easyAdobeAnalytics import query_and_retrieve

def easy_example():
    client_id = '<your-client-id>'
    client_secret = '<your-client-secret'
    company_id = 'company_id'
    rsid = "report_suite_id"
    elements = ['element_id_1','element_id_2']
    
    metrics = ['metric_id_1','metric_id_2']
    
    segments = ['segment_id_1','segment_id_2']
    query_segments_individually = False # True in case you want each segment to be queried individually.
    date_from = '2024-12-3'
    date_to = '2024-12-17'
    date_granularity = "Day" # Month, Year
    report_data = query_and_retrieve(client_id,
                                     client_secret,
                                     elements,
                                     metrics,segments,
                                     rsid,date_from,
                                     date_to,
                                     date_granularity,
                                     company_id,
                                     query_segments_individually)
    print(report_data.head())

if __name__ == '__main__':
    easy_example()
```