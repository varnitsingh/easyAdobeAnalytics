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