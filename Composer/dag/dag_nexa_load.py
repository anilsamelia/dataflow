#########################################################################################################################################
"""                                                                                                                                   """
"""                       SCRIPT NAME      : DAG_nexa_LOAD.py                                                                        """
"""                       VERSION          : v2                                                                                       """
"""                       AUTHOR           : ANIL KUMAR <anilsamelia7@gmail.com>                                                      """
"""                       Contributor      : Anil kumar                                                                               """
"""                       DATE OF CREATION : JAN 27 2024                                                                              """ 
"""                       PURPOSE          : LOADS THE QUARTERLY DATA TO BIGQUERY DATA STORE.                                         """
"""                                                                                                                                   """
#########################################################################################################################################

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
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator
from google.cloud.logging.handlers import CloudLoggingHandler
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator, DataflowTemplatedJobStartOperator


project_id = os.environ["GCP_PROJECT"]
p_split = project_id.split("-")
env = p_split[1]

af_data = '/home/airflow/gcs/data/'


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

suffix_timestamp = time.strftime("%Y%m%d-%H-%M")


def _nexa_nexa_recon(**kwargs):

#Library Imports
    import os
    import yaml
    import pandas as pd
    import datetime
    import logging    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)    
    logging.info("Library imports are complete")    
    project_id = os.environ["GCP_PROJECT"]
    p_split = project_id.split("-")
    env = p_split[1]
    af_data = '/home/airflow/gcs/data/'
    with open(f"{af_data}config_nexa_load_quarterly.yaml", 'r') as yamlfile:
        cfg = yaml.load(yamlfile, Loader=yaml.BaseLoader)        
    logging.info("Config file import is complete")       
    
#Execution Date Define

    today=datetime.datetime.today()
   
    dataset_id_hum = "{}_10p_q{}".format(cfg[env]['load_year'], cfg[env]['load_quarter'])
    dataset_id_ckd = "{}_ckd_esrd_q{}".format(cfg[env]['load_year'], cfg[env]['load_quarter'])   
    logging.info("Date Definition is complete")      

#Read nexa Count File & Create DataFrames

    df_ckd = pd.read_csv(cfg[env]['staging-bucket-path']+cfg[env]['recon_ckd-new-file-name'])
    df_ckd['DATASET_ID'] = dataset_id_ckd
    df_hum = pd.read_csv(cfg[env]['staging-bucket-path']+cfg[env]['recon_ten-p-new-file-name'])
    df_hum['DATASET_ID'] = dataset_id_hum
    df_final_dict=pd.read_excel(cfg[env]['bq-schema-java']['final-data-dict-path'])    
    TGT_TBL=project_id+".nexa_load.nexa_Data_Load_Balancing"
    logging.info("DateFrames Loading is complete")
    print(df_ckd)
    print(df_hum)
    
#Column Name data cleansing
    
    df_hum.columns=df_hum.columns.str.strip().str.replace(' ','_',regex=True).str.upper().str.replace('[#,@,&,:,.,-]', '',regex=True)
    df_ckd.columns=df_ckd.columns.str.strip().str.replace(' ','_',regex=True).str.upper().str.replace('[#,@,&,:,.,-]', '',regex=True)
    df_final_dict.columns=df_final_dict.columns.str.strip().str.replace(' ','_',regex=True).str.upper().str.replace('[#,@,&,:,.,-]', '',regex=True)
    logging.info("Column Names are streamlined")


#Union CKD & HUM DataFrames

    df_union = pd.concat([df_hum, df_ckd])
    df_final_dict=df_final_dict.rename(columns={'nexa_COUNT': 'TABLE_NAME'})
    df = pd.merge(df_union,df_final_dict,how="inner",on='TABLE_NAME')
    df['PROJECT_ID'] =project_id 
    df['TOTAL_COUNT'] = df['TOTAL_COUNT'].astype(str)
    logging.info("Union of CKD & HUM dataframes is completed")
    
    
    
#Create Count Statement for Bigquery
    
    df['nexa_COUNT'] = "select '"+project_id+"' as PROJECT_ID,'"+df["DATASET_ID"]+"' as DATASET_ID, '"+df['DATA_MART_TABLE']+"'  as TABLE_ID,'"+df['TOTAL_COUNT']+"' as nexa_ROW_COUNT,count(*) AS nexa_ROW_COUNT,cast(format_datetime('%Y%m%d%H%M%S',current_datetime('America/New_York')) as numeric) as LOAD_DT from `" +project_id + "." + df['DATASET_ID'] + "."+df['DATA_MART_TABLE']+"`"
    logging.info("Recon SQL Statements are created now")
    
    
#Loading Bigquery Reconciliation Table

    from google.cloud import bigquery
    client = bigquery.Client()
    df_rc = pd.DataFrame()
    for tab_nm in df['nexa_COUNT'].values:
        sql = tab_nm        
        df_x = client.query(sql, project=project_id).to_dataframe()
        df_x['nexa_ROW_COUNT'] = df_x['nexa_ROW_COUNT'].astype(float).astype(int)
        df_rc=pd.concat([df_rc, df_x]) 
    job = client.load_table_from_dataframe( df_rc, TGT_TBL)
    job.result()  

    sql_dups_del="""
    CREATE OR REPLACE TABLE `"""+project_id+""".nexa_load.nexa_Data_Load_Balancing` AS (
    select project_id,dataset_id,table_id,nexa_row_count,nexa_row_count,load_dt 
    from(select *,ROW_NUMBER() OVER (PARTITION BY project_id,dataset_id,table_id ORDER BY load_dt desc ) RNK 
    FROM `"""+project_id+""".nexa_load.nexa_Data_Load_Balancing`   ) x where RNK=1 )
    """
    del_dups=client.query(sql_dups_del, project=project_id)
    results = del_dups.result()
    
    logging.info("Count reconciliation completed Now - Hurray !!")
    del df_rc
    del df

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
        'dag_nexa_load',
        template_searchpath=af_data,
        default_args = default_args,
        start_date=days_ago(0),
        description='This DAG loads nexa Snapshot data to BigQuery for Analytics',
        schedule_interval = None,
        tags=['nexa'],
) as dag_template:
    cloud_logger.info("DAG_nexa_LOAD dag begins now")

########################################
#        FILE DECOMPRESS               #
########################################

    t1_file_decompress = DataflowStartFlexTemplateOperator(
        task_id = 't1_file_decompress',
        gcp_conn_id =cfg[env]['dataflow']['gcp-conn-id'],
        body={
            "launchParameter": {
                "containerSpecGcsPath": cfg[env]['bulk-decompress-df']['dataflow-template-path'],
                "jobName": 'nexa-files-decompress' + '-' + suffix_timestamp,
                "parameters": {
                    'serviceAccount': cfg[env]['dataflow']['service-account'],
                    'inputFilePattern': cfg[env]['staging-bucket-path'] + cfg[env]['bulk-decompress-df']['folder']['input-file-pattern'],
                    'outputDirectory': cfg[env]['staging-bucket-path'] + cfg[env]['bulk-decompress-df']['folder']['output'],
                    'errorDirectory': cfg[env]['staging-bucket-path'] + cfg[env]['bulk-decompress-df']['folder']['error'],
                    'subnetwork': cfg[env]['dataflow']['subnetwork'],
                    'usePublicIps': "false",
                },
            }
        },
        location=cfg[env]['location'],
    )
    
########################################
#           FILE RENAME                #
########################################
    
    t2_src_file_rename = BashOperator(
        task_id = "t2_src_file_rename",
        bash_command = "gsutil ls " + cfg[env]['staging-bucket-path']+cfg[env]['bulk-decompress-df']['folder']['output'] +" > " + af_data+"list_files.txt"+ " && " +af_data+'nexa_src_file_rename.sh ',
    )
    
    
#########################################################################
# COPY WILD CARD DATA DICTIONARY TO A SPECIFIC NAME TO READ BY JAVA JOB #
#########################################################################

    cloud_logger.info("Start: create wildcard data-dictionary file")

    cp_src = cfg[env]['staging-bucket-path'] + cfg[env]['bq-schema-java']['raw-data-dict-wild-card']
    cp_dest = cfg[env]['staging-bucket-path'] + cfg[env]['bq-schema-java']['raw-data-dict-path']

    ckd_table_wild_card = cfg[env]['staging-bucket-path'] + cfg[env]['recon_ckd-wild-card-file-name']
    ckd_table_new_name = cfg[env]['staging-bucket-path'] + cfg[env]['recon_ckd-new-file-name']
   
    ten_p_table_wild_card = cfg[env]['staging-bucket-path'] + cfg[env]['recon_ten-p-wild-card-file-name']
    ten_p_table_new_name = cfg[env]['staging-bucket-path'] + cfg[env]['recon_ten-p-new-file-name']

    t3_data_dict_sync = BashOperator( 
        task_id = "t3_data_dict_sync",
        bash_command ='gsutil cp ' + cp_src + ' ' + cp_dest+ " && "+'gsutil cp ' + ten_p_table_wild_card + ' ' +  ten_p_table_new_name+ " && "+'gsutil cp ' + ckd_table_wild_card + ' ' +  ckd_table_new_name,

    )
    cloud_logger.info("End: create wildcard data-dictionary file") 

    
########################################
# DEFINE BQ TABLE SCHEMA               #
########################################
    
    input_data = json.dumps({
        'projectId':  project_id,
        'rawDataDictFile': cfg[env]['staging-bucket-path'] + cfg[env]['bq-schema-java']['raw-data-dict-path'],
        'finalDataDictRemoteFile': cfg[env]['bq-schema-java']['final-data-dict-path'],
        'loadQueriesFile': af_data + cfg[env]['bq-schema-java']['sql-file'],
        'location': cfg[env]['location'],
        'loadYear': cfg[env]['load_year'],
        'loadQuarter': cfg[env]['load_quarter'],
    })

    unique_filename = str(uuid.uuid4())+'.json'
    with open(unique_filename, 'w') as file:
        json.dump(input_data, file)

    cloud_logger.info("BQ Schema Generator Project Id :" +project_id)
    cloud_logger.info("BQ Schema Generator BucketName :" + cfg[env]['staging-bucket-name'])
    
    
########################################
# BQ TABLE SCHEMA GENERATOR            #
########################################    
    
    t4_bq_schema_gen = BeamRunJavaPipelineOperator(
        task_id = 't4_bq_schema_gen',
        jar = cfg[env]['bq-schema-java']['jar-path'],
        pipeline_options = {
            'input_file': unique_filename,
        },
        job_class='org.springframework.boot.loader.JarLauncher'
    )
    
########################################
# BIGQUERY RAW TABLE LOAD DATAFLOW     #
########################################

    t5_bq_upload_job = DataflowStartFlexTemplateOperator(
        task_id = 't5_bq_upload_job' ,
        gcp_conn_id=cfg[env]['dataflow']['gcp-conn-id'],
        body={
            "launchParameter": {
                "containerSpecGcsPath": cfg[env]['bq-upload-df']['dataflow-template-path'],
                "jobName": 'nexa-bq-raw-tab-upload' + '-' + suffix_timestamp,
                "parameters": {
                    'serviceAccount': cfg[env]['dataflow']['service-account'],
                    'sourceLocation': cfg[env]['staging-bucket-path'] + cfg[env]['bq-upload-df']['folder']['source'],
                    'archiveLocation': cfg[env]['staging-bucket-path'] + cfg[env]['bq-upload-df']['folder']['archive'],
                    'exceptionLocation': cfg[env]['staging-bucket-path'] + cfg[env]['bq-upload-df']['folder']['error'],
                    'configFileLocation': cfg[env]['bq-upload-df']['config-file-path'],
                    'loadYear': cfg[env]['load_year'],
                    'loadQuarter': cfg[env]['load_quarter'],
                    'subnetwork': cfg[env]['dataflow']['subnetwork'],
                    'usePublicIps': "false",
                },
            }
        },
        location=cfg[env]['location'],
    )    

##########################################################
#       BIGQUERY LOAD MART TABLE DATA FROM RAW TABLES    #
##########################################################
    
    t6_bq_load_mart_tables = BigQueryInsertJobOperator(
        task_id = 't6_bq_load_mart_tables' ,
        configuration={
            "query": {
                "query": "{% include '"+cfg[env]['bq-sql-load']['sql-file']+"' %}",
                "useLegacySql": False,
            }
        },
        location=cfg[env]['location'],
    )
    

    
######################################################
#             BIG QUERY RECONCILIATION               #
######################################################


    t7_bq_recon = PythonVirtualenvOperator(
        task_id="t7_bq_recon",
        python_version='3.8',
        python_callable=_nexa_nexa_recon,
        system_site_packages=False,
        provide_context = True,
        requirements=["fsspec","pyyaml","pandas","gcsfs","openpyxl","google-cloud","google-cloud-bigquery","google-cloud-storage","pyarrow" ,],
    )

########################################
# TASK DEPENDENCY SET UP               #
########################################
    
    
    t0_start = DummyOperator(task_id='t0_start')
    t999_end = DummyOperator(task_id='t999_end')
    
    t0_start >> t1_file_decompress >> t2_src_file_rename >> t3_data_dict_sync >> t4_bq_schema_gen >> t5_bq_upload_job >> t6_bq_load_mart_tables >> t7_bq_recon >> t999_end