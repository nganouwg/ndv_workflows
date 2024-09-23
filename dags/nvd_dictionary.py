from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

with DAG (
    dag_id="NVDdatafeed_settings",
    params={
        'requestAllProducts': False,
        'workingDirectory': "/Users/georgesnganou/Documents/Projects/Data/nvd/"
    }
) as dag:
    
    @task.python
    def display_params_task(params: dict):
        dag.log.info(dag.params['requestAllProducts'])


if __name__ == "__main__":
     dag.test(
         run_conf={"requestAllProducts": False}
     )