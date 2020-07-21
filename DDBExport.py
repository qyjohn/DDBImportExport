"""
This is a python script to export data from DynamoDB table into JSON files. 

Usage:
  python DDBExport.py -r <region_name> -t <table_name> -p <process_count>

Example:
  python DDBExport.py -r us-east-1 -t TestTable -p 8
  
The script launches multiple processes to do the work. Each process Scans its own
segment from the DynamoDB table, and writes the output to its own JSON file. 

It is safe to use 1 process per vCPU core. If you have an EC2 instance with 4 vCPU 
cores, it is OK to set the process count to 4. 
"""

#!/usr/bin/python
import sys
import json
import time
import boto3
import multiprocessing
import getopt
import decimal

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj)
    raise TypeError
   
def ddbExportWorker(id, region, table, total_segments):
  """
  We create one DynamoDB client per worker process. This is because boto3 session 
  is not thread safe. 
  """
  session = boto3.session.Session()
  dynamodb    = session.resource('dynamodb', region_name = region)
  ddb_table   = dynamodb.Table(table)
  
  """
  Output filename is table-id.json.
  """
  filename = str(table) + '-' + "{:03d}".format(id) + '.json'
  out=open(filename, 'w')
  """
  Keep on scanning the segment until 
  """
  response = ddb_table.scan(TotalSegments=total_segments, Segment=id)  
  for item in response['Items']:
    out.write(json.dumps(item, default=decimal_default) + '\n')
  while 'LastEvaluatedKey' in response:
    response = ddb_table.scan(TotalSegments=total_segments, Segment=id, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
      out.write(json.dumps(item, default=decimal_default) + '\n')

"""
At the beginning, nothing is defined. Enforce user-supplied values.
"""
region = None
table  = None
process_count = None
"""
Obtain the AWS region, table name, and the number of worker processes from command line.
"""
argv = sys.argv[1:]
opts, args = getopt.getopt(argv, 'r:t:p:')
for opt, arg in opts:
  if opt == '-r':
    region = arg
  elif opt == '-t':
    table = arg  
  elif opt == '-p':
    process_count = int(arg)
"""
Make sure that all command line parameters are defined.
"""
if all([region, table, process_count]) == False:
  print('usage:')
  print('DDBExport.py -r <region_name> -t <table_name> -p <process_count>')
else:
  """
  Launch worker processes to do the work. The worker processes receives data from a
  queue.
  """
  workers = []
  for i in range(process_count):
    p = multiprocessing.Process(target=ddbExportWorker, args=(i, region, table, process_count))
    workers.append(p)
    p.start()
  """
  Wait for worker processes to exit, then the main thread exits.
  """
  for p in workers:
    p.join()
  print("All done.")