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
from google.cloud.logging.resource import Resource
import pandas

# os.environ['TEST'] = 'ping'
# os.environ['THRESHHOLD'] = '4'
# os.environ['METRIC'] = 'Average Latency'
# os.environ['ABOVEBELOW'] = 'above'
# os.environ['MACHINETYPE'] = 'n1-standard-16'
# os.environ['HISTRANGEDAYS'] = '10'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_key.json'
# os.environ['GCP_PROJECT'] = 'smu-benchmarking'
# os.environ['FUNCTION_ON'] = 'True'


def logPkbAlerts(event, context):

    GCP_PROJECT = os.environ.get('GCP_PROJECT')

    logClient = logging.Client()
    bqClient = bigquery.Client('smu-benchmarking')

    if os.environ['FUNCTION_ON'] != 'True':
        return

    # setup logging
    res = Resource(
        type="global",
        labels={
            "location": "us-central1-a",
            "namespace": "pkbAlerts"
        },
    )    
    logger = logClient.logger('pkbAlert')
    
    # setup bq 
    dataset = 'reporting'
    table = 'alerts'
    dst_table = 'pkb_alerts'
    dst_dataset = 'reporting'

        # check if dataset exists, warn otherwise
    try:
        bqClient.get_dataset(dataset)
    except Exception:
        pylog.warn('Dataset doesnt exist: %s' % (dataset))


    query = """
with historic_tests as ( # historic stats for test
SELECT
  max(entry_date) as entry_date,
  sending_region, receiving_region,
  sending_zone, receiving_zone,
  hist_avg, hist_range_days, std_dev,
  hist_avg + (std_dev * @threshhold) as upper_bound,
  hist_avg - (std_dev * @threshhold) as lower_bound,
  test, metric
from `smu-benchmarking.reporting.historic_latency`
where hist_range_days = @histrangedays
 group by sending_region , receiving_region , sending_zone , receiving_zone, hist_avg, hist_range_days, test, metric, std_dev
order by entry_date, sending_region , receiving_region , sending_zone , receiving_zone 
), 
current_tests as ( # test values since last check
    SELECT
      value,
      unit, timestamp,
      TIMESTAMP_MICROS(CAST(timestamp * 1000000 AS int64)) AS thedate,
      #DATE(TIMESTAMP_MICROS(CAST(timestamp * 1000000 AS int64))) AS thedate,
    REGEXP_EXTRACT(labels, r'\|vm_1_zone:(.*?)\|') AS vm1_zone,
    REGEXP_EXTRACT(labels, r'\|vm_2_zone:(.*?)\|') AS vm2_zone,
    REGEXP_EXTRACT(labels, r'\|sending_zone:(.*?)\|') AS sending_zone,
    REGEXP_EXTRACT(labels, r'\|receiving_zone:(.*?)\|') AS receiving_zone,
    REGEXP_EXTRACT(labels, r'\|vm_1_zone:(us-.*?)-.*?\|') AS vm1_region,
    REGEXP_EXTRACT(labels, r'\|vm_2_zone:(us-.*?)-.*?\|') AS vm2_region,
    REGEXP_EXTRACT(labels, r'\|sending_zone:(us-.*?)-.*?\|') AS sending_region,
    REGEXP_EXTRACT(labels, r'\|receiving_zone:(us-.*?)-.*?\|') AS receiving_region,
    test, sample_uri, run_uri , labels
    FROM
      `smu-benchmarking.daily_tests.daily_1`
    where
    timestamp  > ( 
    SELECT max(sample_timestamp) FROM `smu-benchmarking.reporting.pkb_alerts`
    where test = @test and metric = @metric 
    )    
    and test = @test
    AND REGEXP_EXTRACT(labels, r'\|vm_1_machine_type:(.*?)\|') = @machinetype
    AND metric = @metric
    )
#select count(*) from(    
#SELECT TO_JSON_STRING(t, true) from(
select historic_tests.sending_region as s_reg, historic_tests.receiving_region as r_reg, historic_tests.sending_zone as s_zone, historic_tests.receiving_zone as r_zone, round(hist_avg, 2) as hist_avg,round(historic_tests.upper_bound, 3) as threshhold, round(current_tests.value, 2) as sample_value, current_tests.value > historic_tests.upper_bound as alert, string(current_tests.thedate) as sample_date, current_tests.test, historic_tests.metric, cast(historic_tests.entry_date as string) as hist_entry_date, historic_tests.std_dev, CONCAT(CAST(@threshhold as STRING),' std devs ', @abovebelow, ' mean') as alert_condition,  current_tests.run_uri, current_tests.sample_uri, current_tests.timestamp as sample_timestamp , current_tests.labels
from historic_tests
join current_tests on
historic_tests.sending_region = current_tests.sending_region
and historic_tests.receiving_region = current_tests.receiving_region
and historic_tests.sending_zone = current_tests.sending_zone
and historic_tests.receiving_zone = current_tests.receiving_zone
and historic_tests.test = current_tests.test
and current_tests.value > historic_tests.upper_bound
order by sample_timestamp
"""
    query_params = [
        #bigquery.ScalarQueryParameter("test", "STRING", "ping"),
        bigquery.ScalarQueryParameter("test", "STRING", os.environ.get("TEST", "ping")),
        #bigquery.ScalarQueryParameter("metric", "STRING", "Average Latency"),
        bigquery.ScalarQueryParameter("metric", "STRING", os.environ.get("METRIC", "Average Latency")),
        #bigquery.ScalarQueryParameter("threshhold", "INT64", "3"),
        bigquery.ScalarQueryParameter("threshhold", "INT64", os.environ.get("THRESHHOLD", "3")),
        #bigquery.ScalarQueryParameter("abovebelow", "STRING", "above"),
        bigquery.ScalarQueryParameter("abovebelow", "STRING", os.environ.get("ABOVEBELOW", "above")),
        #bigquery.ScalarQueryParameter("machinetype", "STRING", "n1-standard-16"),
        bigquery.ScalarQueryParameter("machinetype", "STRING", os.environ.get("MACHINETYPE","n1-standard-16")),
        #bigquery.ScalarQueryParameter("histrangedays", "INT64", 10),
        bigquery.ScalarQueryParameter("histrangedays", "INT64", os.environ.get("HISTRANGEDAYS", "10")),
        
        #bigquery.ScalarQueryParameter("last_checked_timestamp", "TIMESTAMP", datetime.datetime(2019, 1, 2, 8, 0, tzinfo=pytz.UTC)),
    ]

    # Query Config
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
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

    # quit if no results
    if len(results.index) == 0:
        print("No New Alerts")
        return

    # Insert generated Alerts into bigquery table
    insert_table_id = 'smu-benchmarking.reporting.pkb_alerts'
    table = bqClient.get_table(insert_table_id)
    errors = [[]]
    errors = bqClient.insert_rows_from_dataframe(table, results)
    if errors[0] == []:
        print(f"{results.shape[0]} new rows have been added to smu-benchmarking.reporting.pkb_alerts")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


    # Crudely iterate over rows in dataframe
    rows = results.iterrows()

    while True:
        count = 0
        iteratorExhausted = False

        with logger.batch() as blogger:
            while count < 100:
                try:
                    item = next(rows)
                except StopIteration:
                    iteratorExhausted = True
                    break  # Iterator exhausted: stop the loop
                else:
                    blogger.log_struct(
                        {"message": "PKB Alert",  "labels": dict(item[1])}, resource=res
                    )
              
                count += 1

        if iteratorExhausted:
            break

    return 'Alert: Avg latency '

# logPkbAlerts(None, None)


