######################################################################################################################################################################
"""                                                                                                                                                                """
"""                       SCRIPT NAME      : SFTP_PULL_ARCHIVE.py                                                                                        """
"""                       DATE OF CREATION : FEB 12 2025                                                                                                           """ 
"""                       PURPOSE          : This DAG is responsible for downloading files from SFTP server to Landing bucket and create a copy in Archive folder. """
"""                       AUTHOR           : ANIL KUMAR <anilsamelia7@gmail.com>                                                                                   """
"""                                                                                                                                                                """
######################################################################################################################################################################

######################################
# IMPORT PYTHON & AIRFLOW MODULES    #
######################################

import os
import yaml
import uuid
import json
import time
import logging
import datetime
import pandas as pd
from airflow import models
import google.cloud.logging
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from google.cloud.logging.handlers import CloudLoggingHandler
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator, DataflowTemplatedJobStartOperator


af_data = '/home/airflow/gcs/data/'

project_id = os.environ["GCP_PROJECT"]
p_split = project_id.split("-")
env = p_split[1]

with open(f"{af_data}config_nexa_load_quarterly.yaml", 'r') as yamlfile:
    cfg = yaml.load(yamlfile, Loader=yaml.BaseLoader)

######################################
# SETTING CLOUD LOGGING PARAMETER    #
######################################

client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)
cloud_logger = logging.getLogger('cloudLogger')
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(handler)

########################################
# SETTING VARIABLE FOR BIGQUERY UPLOAD #
########################################

today=datetime.datetime.today()
quarter = pd.Timestamp(today).quarter 
year = pd.Timestamp(today).year 

archive_folder = cfg[env]['load_year'] + "-Q" + cfg[env]['load_quarter']
suffix_timestamp = time.strftime("%Y%m%d-%H-%M")


########################################
# DEFINING COMPOSER VARIABLES          #
########################################

default_args = {
    'owner': 'nexa',
    'depends_on_past': False,
    'email': 'composer@nexa.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
    'default_view': 'graph'
}

with models.DAG(
        'dag_nexa_sftp_pull_archive',
        template_searchpath=af_data,
        default_args = default_args,
        start_date=days_ago(0),
        description='This DAG is responsible for downloading files from SFTP server to Landing bucket and create a copy in Archive folder',
        schedule_interval = None,
        tags=['nexa'],
) as dag_template:
    cloud_logger.info("DAG_nexa_SFTP_PULL_ARCHIVE begins now")
    
    


######################################################
# SFTP DOWNLOAD FILES FROM SERVER TO ARCHIVED FOLDER #
######################################################


    t1_sftp_files_download = DataflowStartFlexTemplateOperator(
            task_id = 't1_sftp_files_download' ,
            gcp_conn_id =cfg[env]['dataflow']['gcp-conn-id'],
            body={
                "launchParameter": {
                    "containerSpecGcsPath": cfg[env]['sftp-df']['dataflow-template-path'],
                    "jobName": 'nexa-sftp-files-download' + '-' + suffix_timestamp ,
                    "parameters": {
                        'serviceAccount': cfg[env]['dataflow']['service-account'],
                        'projectId': project_id,
                        'host': cfg[env]['sftp-df']['host'],
                        'username': cfg[env]['sftp-df']['username-secret-key'],
                        'password': cfg[env]['sftp-df']['password-secret-key'],
                        'inputFolder': cfg[env]['sftp-df']['folder']['input'],
                        'inputFilePattern':cfg[env]['sftp-df']['folder']['input-file-pattern'],
                        'outputFolder':  cfg[env]['staging-bucket-path'] + cfg[env]['sftp-df']['folder']['output'],
                        'concurrency': cfg[env]['sftp-df']['concurrency'],
                        'subnetwork': cfg[env]['dataflow']['subnetwork'],
                        'usePublicIps': 'false',
                    },
                }
            },
            location=cfg[env]['location'],
            #retries=10,
        )
        
######################################################
#  CREATE A COPY OF SOURCE FILES IN ARCHIVE FOLDER   #
######################################################


    t2_create_copy_in_archive = GCSSynchronizeBucketsOperator(
            task_id = 't2_create_copy_in_archive',
            source_bucket = cfg[env]['copy-to-archive']['bucket']['source'],
            destination_bucket = cfg[env]['copy-to-archive']['bucket']['destination'],
            source_object = cfg[env]['copy-to-archive']['folder']['source'],
            destination_object = cfg[env]['copy-to-archive']['folder']['destination']  + archive_folder + '/',
            delete_extra_files=False,
            allow_overwrite=True,
            recursive=True,
       )
       
########################################
# TASK DEPENDENCY SET UP               #
########################################     
    
    t0_start = DummyOperator(task_id='t0_start')
    t999_end = DummyOperator(task_id='t999_end')
    
    
    
    t0_start >> t1_sftp_files_download >> t2_create_copy_in_archive >> t999_end