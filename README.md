# Reddit-ETL

![architecture_3x_light](https://github.com/user-attachments/assets/3cc03e38-b4c7-4a5c-a507-b530210d9b84)

## Extract
In this phase data is extracted from Reddit API using Apache Airflow and is dumped into a csv file.
This csv file is then pushed to Azure Data Lake Storage in raw container.

## Transform
Azure Databricks pulls the csv data stored in raw container and cleans the data, does data validity checks.
Additionally perfomrs some transformations and pushes the data in parquet format to processed container in ADLS.

## Load
Here I have used Azure Data Factory to pull the data from processed container and push it to Azure SQL database.

## Visualization
I have used Looker studio to build dashboard on top of data stored in Azure SQL.
Dashboard Link : https://lookerstudio.google.com/s/kJ2c-oL05QE

All of the above tasks are Orchestrated using Apache Airflow and DAG is scheduled to run everyday.
Below is snapshot of tasks.
![image](https://github.com/user-attachments/assets/85e2c220-8bb6-42ba-9cdd-4c0eb252b4a4)

