import datetime
import pandas as pd
import pathlib
import praw
import sys
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.core.exceptions import AzureError
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable


# Set up logging
logger = logging.getLogger(__name__)

STORAGE_ACCOUNT_NAME = Variable.get("storage_account_name")
CONTAINER_NAME = Variable.get("container_name") 
DIRECTORY_NAME = Variable.get("directory_name")

TENANT_ID = Variable.get("tenant_id") 
ADLS_CLIENT_ID = Variable.get("adls_client_id") 
CLIENT_SECRET = Variable.get("client_secret")  

# Reddit Configuration Variables
SECRET = Variable.get("reddit_secret") 
CLIENT_ID = Variable.get("reddit_client_id") 

# Options for extracting data from PRAW
SUBREDDIT = "india"
TIME_FILTER = "day"
LIMIT = None

# Fields that will be extracted from Reddit.
POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)

def workflow():
    """Extract Reddit data and load to CSV"""
    reddit_instance = api_connect()
    subreddit_posts_object = subreddit_posts(reddit_instance)
    extracted_data = extract_data(subreddit_posts_object)
    transformed_data = transform_basic(extracted_data)
    load_to_csv(transformed_data)


def api_connect():
    """Connect to Reddit API"""
    try:
        instance = praw.Reddit(
            client_id=CLIENT_ID, client_secret=SECRET, user_agent="My User Agent"
        )
        return instance
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)


# TODO: Improve error handling
def subreddit_posts(reddit_instance):
    """Create posts object for Reddit instance"""
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)
        return posts
    except Exception as e:
        print(f"There's been an issue. Error: {e}")
        sys.exit(1)


# TODO: Improve error handling
def extract_data(posts):
    """Extract Data to Pandas DataFrame object"""
    list_of_items = []
    try:
        for submission in posts:
            to_dict = vars(submission)
            sub_dict = {field: to_dict[field] for field in POST_FIELDS}
            list_of_items.append(sub_dict)
            extracted_data_df = pd.DataFrame(list_of_items)
    except Exception as e:
        print(f"There has been an issue. Error {e}")
        sys.exit(1)

    return extracted_data_df

 # TODO: Remove all but the edited line, as not necessary. For edited line, rather 
 # than force as boolean, keep date-time of last edit and set all else to None.
def transform_basic(df):
    """Some basic transformation of data. To be refactored at a later point."""

    # Convert epoch to UTC
    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s")
    # Fields don't appear to return as booleans (e.g. False or Epoch time). Needs further investigation but forcing as False or True for now.
    df["over_18"] = np.where(
        (df["over_18"] == "False") | (df["over_18"] == False), False, True
    ).astype(bool)
    df["edited"] = np.where(
        (df["edited"] == "False") | (df["edited"] == False), False, True
    ).astype(bool)
    df["spoiler"] = np.where(
        (df["spoiler"] == "False") | (df["spoiler"] == False), False, True
    ).astype(bool)
    df["stickied"] = np.where(
        (df["stickied"] == "False") | (df["stickied"] == False), False, True
    ).astype(bool)
    return df


def load_to_csv(extracted_data_df):
    """Save extracted data to CSV file in /tmp folder"""
    extracted_data_df.to_csv("./reddit_data/data.csv", index=False)

def upload_to_azure_datalake(**context):
    """Upload generated data to Azure Data Lake with enhanced error handling"""
    try:
        # Get file path from previous task
        file_path = "./reddit_data/data.csv"
        logger.info(f"Retrieved file path: {file_path}")
        
        credential = ClientSecretCredential(
            tenant_id=TENANT_ID,
            client_id=ADLS_CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        
        # Initialize the service client
        service_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=credential
        )
        logger.info("Successfully created DataLakeServiceClient")
        
        # Get file system client
        file_system_client = service_client.get_file_system_client(file_system=CONTAINER_NAME)
        logger.info(f"Successfully connected to container: {CONTAINER_NAME}")
        
        # Get directory client
        directory_client = file_system_client.get_directory_client(DIRECTORY_NAME)
        
        # Create the directory if it doesn't exist
        try:
            directory_client.create_directory()
            logger.info(f"Created directory: {DIRECTORY_NAME}")
        except Exception as e:
            logger.info(f"Directory already exists or error creating directory: {str(e)}")
        
        # Generate output filename with timestamp
        output_filename = f"reddit_data_{context['ts_nodash']}.csv"
        
        # Get file client
        file_client = directory_client.create_file(output_filename)
        logger.info(f"Created file client for: {output_filename}")
        
        # Upload the file
        with open(file_path, 'rb') as file:
            file_client.upload_data(file, overwrite=True)
        
        logger.info(f"Successfully uploaded file to Azure Data Lake: {output_filename}")
        return f"Uploaded {output_filename}"
    
    except AzureError as ae:
        logger.error(f"Azure specific error: {str(ae)}")
        raise
    except Exception as e:
        logger.error(f"Error uploading to Azure Data Lake: {str(e)}")
        raise
    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


#DAG
dag = DAG(
    'reddit_data_extraction',
    default_args=default_args,
    description='Pull data from reddit API',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['reddit', 'API']
)

pull_data_task = PythonOperator(
    task_id='pull_data',
    python_callable=workflow,
    provide_context=True,
    dag=dag
)

upload_to_azure_task = PythonOperator(
    task_id='upload_to_azure',
    python_callable=upload_to_azure_datalake,
    provide_context=True,
    dag=dag
)

run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        databricks_conn_id='databricks_default',  # Use the connection ID you set up in Airflow
        job_id='1048923199905887',  # Replace with your Databricks job ID
)

trigger_adf = TriggerDagRunOperator(
        task_id="trigger_adf",
        trigger_dag_id="adf_pipeline_trigger_dag",  # The DAG ID of DAG 2
    )

pull_data_task >> upload_to_azure_task >> run_databricks_job >> trigger_adf


