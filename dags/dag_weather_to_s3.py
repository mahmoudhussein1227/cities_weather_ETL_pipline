import glob as g
import logging
from airflow.sdk import dag , task
from datetime import datetime,timedelta

from scripts.ingest import run 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.providers.postgres.hooks.postgres import PostgresHook


from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import sys


default_args = {
    'owner' : 'mahmoud hussein',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}


country = "germany"
date = str(datetime.now().date()).replace('-' , '_')
dest_file = f"dags/results/{country}_cities.csv"
file      = f"dags/data/{country}_cities.txt"

@dag(default_args = default_args,
     dag_id = "weather_to_aws_S3_v23", 
     description= '''this dag is to ingest weather data from open-meteo api for specific
                   city and stored it in aws S3 bucket as csv file''',
     schedule= '@daily',
     start_date= datetime(2026 , 8 , 4),
     catchup=False,
     is_paused_upon_creation=True)

def weather_to_aws_S3():

    @task()
    def ingest():
        df = run(country= country , dest_file=dest_file , file=file)
        
   # Swapped from task.bash to SparkSubmitOperator for robust Docker execution
   # transformation task
    spark_transform = SparkSubmitOperator(
            task_id="spark_runner",
            application=f"/opt/airflow/dags/scripts/transform.py",
            application_args=[country],
            conn_id="spark_default"  # Ensure connection host is set to local[*]
    )
       
            
        

    # row data from the source (s3 bucket act as bronze layer)
    @task()
    def load():
        hook = S3Hook(aws_conn_id='aws_default_2')
        hook.load_file(filename = dest_file , 
                       key = f"{country}_cities_{date}.csv",
                       bucket_name = "airflow",
                       replace = True) # for testing only must be false in production 

    @task()
    def load_weather_summary():
        hook = PostgresHook(postgres_conn_id = 'postgres_localhost')

        #1 create table if not exisits
        create_sql = f'''create table if not EXISTS {country}_weather_summary_{date}(
                        id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        city varchar(250) ,
                        country varchar (250),
                        date date,
                        max_temp float,
                        min_temp float,
                        mean_temp float,
                        weather_condition varchar(50)) ;

                        truncate table {country}_weather_summary_{date}; '''
        
        hook.run(sql=create_sql)
        #2 load the data from csv to the table
        sql_copy = f'''copy {country}_weather_summary_{date} (city , country , date , max_temp , min_temp , mean_temp , weather_condition)
                    from STDIN
                    with(format csv , header true  , delimiter ',')'''
        folder = f'dags/results/{country}_weather_summary.csv/'
        csv_files = g.glob(folder + '*.csv')
        hook.copy_expert(sql_copy , csv_files[0])


    @task()
    def load_min_max():
        hook = PostgresHook(postgres_conn_id = 'postgres_localhost')
        
        #1 create table if not exisits
        create_sql = f'''create table if not EXISTS {country}_weather_min_max_{date}(
                        id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        city varchar(250) ,
                        country varchar (250),
                        date date,
                        max_temp float,
                        min_temp float);

                        truncate table {country}_weather_min_max_{date};
                    '''
        hook.run(sql=create_sql)

        #2 load the data from csv to the table
        sql_copy = f'''copy {country}_weather_min_max_{date} (city , country , date , max_temp , min_temp)
                    from STDIN
                    with(format csv , header true  , delimiter ',')'''
        folder = f'dags/results/{country}_weather_info.csv/'
        csv_files = g.glob(folder + '*.csv')
        hook.copy_expert(sql_copy , csv_files[0])
           
        
    ingest()>> load () >> spark_transform >>[load_weather_summary() , load_min_max()]
        
weather_dag = weather_to_aws_S3()



