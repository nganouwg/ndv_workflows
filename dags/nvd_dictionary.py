
import time
import requests
import json
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


base_url = None

with DAG (
    dag_id="NVDdatafeed_settings",
    params={
        'apikey': "",
        'Allrecords': False,
        'workingDirectory': "/Users/georgesnganou/Documents/Projects/Data/nvd/",
        'RequestType': Param(
            'CPE', type="string", title="Select Request Type",
            description="CVE - Common Vulnerability and Exposure; CPE - Common Platform Enumeration",
            enum=["CPE", "CVE"],
            values_display={
                "CPE": "Common Platform Enumeration",
                "CVE": "Common Vulnerability and Exposure",
            }
        ),
        'OverrideURL': Param(
            'https://services.nvd.nist.gov/rest/json/cpes/2.0?cpeMatchString=cpe:2.3:o:microsoft:windows_10',
            type = "string",
            title = "API URL Override",
            description="For example, The following link request CPE names for Microsoft Windows 10: \n https://services.nvd.nist.gov/rest/json/cpes/2.0?cpeMatchString=cpe:2.3:o:microsoft:windows_10 "
        ),
        'RequestPerMinute': Param(
            6,
            type = "integer",
            title= "Number of API Request per Minute",
            description="Number from 6 to 10",
            enum=[ i for i in range(6, 11)]
        )

    }
) as dag:
    
    @task.python
    def display_params_task(params: dict):
        dag.log.info(dag.params['requestAllProducts'])

def verify_param():

    pass

def extract():

    #create a processing directory from working direcory paramenter
    processingDir = Path(dag.params["workingDirectory"]).joinpath("processing")
    Path(processingDir).mkdir(parents=True, exist_ok=True)

    now = datetime.now()

    filename = dag.params["RequestType"] + "_" + now.strftime("%Y%m%d_%H%M%S_%f")[:-3] + ".json"
    fullfilename = Path(processingDir).joinpath( filename)

    waittime = 60 / dag.params["RequestPerMinute"]

    if(dag.params["OverrideURL"].strip() is None):
        if(dag.params["RequestType"] == 'CPE'):
            base_url = "https://services.nvd.nist.gov/rest/json/cpes/2.0"
        else:
            base_url = "https://services.nvd.nist.gov/rest/json/cves/2.0"
    else:
        base_url = dag.params["OverrideURL"]

    # From: https://nvd.nist.gov/developers/products 

    #The CPE API returns four primary objects in the response body that are used for pagination: 
    #   resultsPerPage, startIndex, totalResults, and products. 
    #   totalResults indicates the total number of CPE records that match the request parameters. 
    #   If the value of totalResults is greater than the value of resultsPerPage 
    #   there are more records than could be returned by a single API response and 
    #   additional requests must update the startIndex to get the remaining records.

    #   resultsPerPage: This parameter specifies the maximum number of CPE records to be returned in a single API response. 
    #   For network considerations, the default value and maximum allowable limit is 10,000.

    resultsPerPage = 0
    totalresults = 1

    startIndex = 0

    while(totalresults > resultsPerPage):
        response = requests.get(base_url, headers = {
            'apikey': "15093e58-5423-4fd5-b822-e6e90b2233da",
            "startIndex": str(startIndex)
        })

        ndv_jsondata = response.json()

        resultsPerPage = ndv_jsondata['resultsPerPage']
        totalresults = ndv_jsondata['totalResults']

        #TODO: Write while performing subsequest request. 
        #dump the data to file
        with open(fullfilename, "w") as f:
            json.dump(ndv_jsondata, f)

        startIndex = startIndex + resultsPerPage

        time.sleep(waittime)


start = EmptyOperator(task_id="start", dag=dag)
paramverify_task = PythonOperator(task_id="param_verification", python_callable=verify_param, dag=dag)
extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

start >> paramverify_task >> extract_task >> end

if __name__ == "__main__":
     dag.test(
         run_conf={
                'apikey': "",
                'Allrecords': False,
                'workingDirectory': "/Users/georgesnganou/Documents/Projects/Data/nvd/",
                'RequestType': "CPE",
                'OverrideURL': 'https://services.nvd.nist.gov/rest/json/cpes/2.0?cpeMatchString=cpe:2.3:o:microsoft:windows_10',
                'RequestPerMinute': 6,
            }
     )

