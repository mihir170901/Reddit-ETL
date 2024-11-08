from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id="adf_pipeline_trigger_dag",
    default_args=default_args,
    description="A DAG to trigger an ADF pipeline",
    schedule_interval=None,  # Define your schedule or use None for manual runs
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to run ADF pipeline
    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline_task",
        pipeline_name="pl_adls_to_sql",  # Replace with your ADF pipeline name
        resource_group_name="RCS",  # Replace with your resource group name
        factory_name="airflow-mihir-adf",  # Replace with your Data Factory name
        parameters={},  # Optional: Add pipeline parameters here if needed
    )

    run_adf_pipeline
