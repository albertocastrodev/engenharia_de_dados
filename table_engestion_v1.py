import sys
import json
import boto3
import pandas as pd
from math import ceil
from datetime import datetime

from awsglue.transforms import * 
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import * 

args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_TARGET',
                                    'S3_SOURCE', 'S3_JSON', 
                                    'CRAWLER_TABLE', 'FILE_NAME',
                                    'CRAWLER_DATABASE','LOG_PATH',
                                    'JSON_FILE', 'PROCCESS_TYPE'])
                    

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(['JOB_NAME'], args)

s3 = boto3.resource('s3')

job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
bucket_source = args ['S3_SOURCE']
bucket_target = args['S3_TARGET']
bucket_json = args['S3_JSON']
crawler_catalog = args['CRAWLER_TABLE']
crawler_database = args['CRAWLER_DATABASE']
json_file = args['JSON_FILE']
file_name = args['FILE_NAME']
log_path = args['LOG_PATH']
proccess_type = args['PROCCESS_TYPE']

#################################################
# Function definitions

def save_log (proccess_type, job_run_id, status, n_rows, start_time, end_time, elapsed_time):
    log_data = {
        'process_type': proccess_type,
        'job_id': job_run_id,
        'status': status,
        'total_registers':n_rows,
        'start_time': start_time,
        'end_time': end_time,
        'elapsed_time': elapsed_time
    }
    
    try:
        df1 = pd.read_csv('{}{}.csv'.format(log_path,job_name))
        df2 = pd.DataFrame([log_data])
        df3 = df1.append(df2)
        df3.to_csv('{}{}.csv'.format(log_path, job_name), index = False)
    
    except:
        df = pd.DataFrame([log_data])
        df.to_csv('{}{}.csv'.format(log_path, job_name), index = False)

def lower_column_names(df):
    return df.toDF(*[c.lower() for c in df.columns])

def standarlize_column_names(df):
    return df.toDF(*[c.strip("\t") for c in df.columns])

def get_struct_type (column_name, schema):
    for field in schema['columns']:
        if column_name.lower() == field["name"].lower():
            return field

def load_json(bucket_name, bucket_key):
    content_object = s3.Object(bucket_name, bucket_key)
    file_content = content_object.get()['Body'].read().decode("utf-8")
    json_file = json.loads(file_content)
    return json_file

def load_csv(path, col, spark):
    df = spark.read.option("delimiter", ",") \
                   .option("header", True) \
                   .csv(path)
    df = df.toDF(*col)
    return df

#######################################

#PRINCIPAL PARÂMETRO
MAX_REGISTERS_REPARTITION = 250000

#INÍCIO DO SCRIPT
start_time = datetime.now()
start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
print(f"Script Start: {start_time_str}")

#LOAD JSON
json_schema = load_json(bucket_json,json_file)
columns = [x["name"] for x in json_schema["columns"]]

