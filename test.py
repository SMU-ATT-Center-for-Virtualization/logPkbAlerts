"""Import a json file into BigQuery."""

import logging as pylog
import os
import re
import json

import datetime
#import pytz

from flask import escape
from google.cloud import bigquery
from google.cloud import logging
# from google.cloud.logging.resource import Resource
import pandas

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_key.json'
# GCP_PROJECT = os.environ.get('GCP_PROJECT')

# logClient = logging.Client()
# bqClient = bigquery.Client('smu-benchmarking')

# setup bq 
# dataset = 'reporting'
# table = 'alerts'
# dst_table = 'pkb_alerts'
# dst_dataset = 'reporting'

    # check if dataset exists, warn otherwise
# try:
#     bqClient.get_dataset(dataset)
# except Exception:
#     pylog.warn('Dataset doesnt exist: %s' % (dataset))

#TODO put in current_test.sample_timestamp >= historic_tests.entry_date

bqClient = bigquery.Client('smu-benchmarking')

query = """
CALL `smu-benchmarking.daily_tests.test_get_n2_standard_80_ping`();
"""
# query_params = [
#     bigquery.ScalarQueryParameter("test", "STRING", "ping"),
#     # bigquery.ScalarQueryParameter("test", "STRING", os.environ.get("TEST", "ping")),
#     bigquery.ScalarQueryParameter("metric", "STRING", "Average Latency"),
#     bigquery.ScalarQueryParameter("threshhold", "INT64", os.environ.get("LATENCY_THRESHHOLD", "3")),
#     bigquery.ScalarQueryParameter("percentage_threshhold", "FLOAT", os.environ.get("LATENCY_PERCENTAGE_THRESHHOLD", "0.05")),
#     bigquery.ScalarQueryParameter("machinetype", "STRING", os.environ.get("MACHINETYPE","n1-standard-16")),
#     bigquery.ScalarQueryParameter("histrangedays", "INT64", os.environ.get("HISTRANGEDAYS", "10")),
# ]

# Query Config
job_config = bigquery.QueryJobConfig()
# job_config.query_parameters = query_params
# table_ref = bqClient.dataset('reporting').table('pkb_alerts')
# job_config.destination = table_ref
# job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

query_job = bqClient.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="US",
    job_config=job_config,
)  # API request - starts the query

# Print the results
count = 0
results = query_job.result().to_dataframe()

print(results.shape)
print(results)
# quit if no results
if len(results.index) == 0:
    print("No New Alerts")
    

# Insert generated Alerts into bigquery table
# insert_table_id = 'smu-benchmarking.reporting.pkb_alerts'
# table = bqClient.get_table(insert_table_id)
# errors = [[]]
# errors = bqClient.insert_rows_from_dataframe(table, results)
# if errors[0] == []:
#     print(f"{results.shape[0]} new rows have been added to smu-benchmarking.reporting.pkb_alerts")
# else:
#     print("Encountered errors while inserting rows: {}".format(errors))


# Crudely iterate over rows in dataframe
rows = results.iterrows()