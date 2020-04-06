#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext

sc = pyspark.SparkContext()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the InputFormat. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input'.format(bucket)

conf = {
    # Input Parameters.
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': 'big-liberty-271017', 
    'mapred.bq.input.dataset.id': 'ca687_dataset',
    'mapred.bq.input.table.id': 'covid19',
}

# Output Parameters.
output_dataset = 'covid19_dataset'
output_table = 'total_output'

# Load data in from BigQuery.
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)

# Perform item count.
total_count = (
    table_data
    .map(lambda record: json.loads(record[1]))
    .map(lambda x: (x['Country_Region'].lower(), int(x['Confirmed'])))
    .reduceByKey(lambda x, y: x + y))

# Display 10 results.
pprint.pprint(total_count.take(1000))

# Stage data formatted as newline-delimited JSON in Cloud Storage.
output_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_output'.format(bucket)
output_files = output_directory + '/part-*'

sql_context = SQLContext(sc)
(total_count
 .toDF(['Country_Region', 'Confirmed'])
 .write.format('json').save(output_directory))

# Shell out to bq CLI to perform BigQuery import.
subprocess.check_call(
    'bq load --project_id=big-liberty-271017 --source_format NEWLINE_DELIMITED_JSON '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table, files=output_files
    ).split())

# Manually clean up the staging_directories, otherwise BigQuery
# files will remain indefinitely.
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)