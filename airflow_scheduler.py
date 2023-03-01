#!/usr/bin/python
# -*- coding: utf-8 -*-
# import the libraries

from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG

from airflow import DAG

# Operators; we need this to write tasks!

from airflow.operators.bash_operator import BashOperator

# This makes scheduling easy

from airflow.utils.dates import days_ago

# defining DAG arguments

# You can override them on a per-task basis during operator initialization
##A typical DAG Arguments block looks like this.
##Task 1.1 -Define DAG arguments

default_args = {
    'owner': 'kaushik Dey',
    'start_date': days_ago(0),
    'email': ['abc.kas@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

# Task 1.2- Define the DAG
# A typical DAG definition block looks like this.

dag = DAG(dag_id='ETL_toll_data', default_args=default_args,
          description='Apache Airflow Final Assignment',
          schedule_interval=timedelta(days=1))

# define tasks
# Task 1.3- Create a task named unzip_data
# saved as unzip_data.jpg
    # ---------------------------

unzip_data = BashOperator(task_id='unzip_data',
                          bash_command='tar -zxvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment'
                          , dag=dag)

    # Task 1.4- Create a task to extract data from csv file
    # file saved as extract_data_from_csv.jpg

extract_data_from_csv = BashOperator(task_id='extract_data_from_csv',
        bash_command='cut -d"," -f 1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv'
        , dag=dag)

# Task 1.5 - Create a task to extract data from tsv file.

extract_data_from_tsv = BashOperator(task_id='extract_data_from_tsv',
        bash_command='cut -f 5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv --output-delimiter=","'
        , dag=dag)

# Task 1.6 - Create a task to extract data from fixed width file

extract_data_from_fixed_width = \
    BashOperator(task_id='extract_data_from_fixed_width',
                 bash_command='cut -c 59-61,63-68 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv --output-delimiter=","'
                 , dag=dag)

# Task 1.7 - Create a task to consolidate data extracted from previous tasks

consolidate_data = BashOperator(task_id='consolidate_data',
                                bash_command='paste /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv'
                                , dag=dag)

# Task 1.8 - Transform and load the data

transform_data = BashOperator(task_id='transform_data',
                              bash_command='awk \'BEGIN{FS=","; OFS=","} {print $1,$2,$3,toupper($4),$5,$6,$7,$8,$9}\' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv'
                              , dag=dag)

# task pipeline
# Task 1.9 - Define the task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv \
    >> extract_data_from_fixed_width >> consolidate_data \
    >> transform_data
