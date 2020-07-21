"""
This is a python script to load data from JSON file into DynamoDB table. 

Usage:
  python DDBImport.py -r <region_name> -t <table_name> -s <source_file> -p <process_count>

Example:
  python DDBImport.py -r us-east-1 -t TestTable -s test.json -p 8
  
The script launches multiple processes to do the work. The processes poll from a
common queue for data to write. When the queue is empty, the processes continues to 
poll the queue for another 60 seconds to make sure it does not miss anything. 

It is safe to use 1 process per vCPU core. If you have an EC2 instance with 4 vCPU 
cores, it is OK to set the process count to 4. The BatchWriteItem API is used to
perform the import. Depending on the size of the items, each process can consume
approximately 1000 WCU during the import. 

Tested on an EC2 instance with the c3.8xlarge instance type. The data set contains 
10,000,000 items, with each item being approximately 170 bytes. The size of the JSON 
file is 1.7 GB. The DynamoDB table has 40,000 provisioned WCU. Perform the import 
with 64 threads, and the import is completed in 7 minutes. The peak consumed WCU is 
approximately 32,000 (average value over a 1-minute period).
"""

#!/usr/bin/python
import sys
import json
import time
import boto3
import multiprocessing
import Queue
import getopt

   
def ddbImportWorker(id, region, table, queue):
  """
  We create one DynamoDB client per worker process. This is because boto3 session 
  is not thread safe. 
  """
  session = boto3.session.Session()
  dynamodb    = session.resource('dynamodb', region_name = region)
  ddb_table   = dynamodb.Table(table)
  """
  Keep on polling the queue for items to work on. 
  Use BatchWriteItem to write items in batches into the DynamoDB table. In boto3, 
  the DynamoDB.Table.batch_writer() automatically handles buffering and sending 
  items in batches, and automatically handles any unprocessed items and resends 
  them when needed. 
  """
  with ddb_table.batch_writer() as batch:
    work = 1
    while work == 1:
      try:
        item = queue.get(timeout=60)
        batch.put_item(Item=item)
      except Queue.Empty:
        work = 0  


"""
At the beginning, nothing is defined. Enforce user-supplied values.
"""
region = None
table  = None
source = None
process_count = None
"""
Obtain the AWS region, table name, source file, and the number of worker processes
from command line.
"""
argv = sys.argv[1:]
opts, args = getopt.getopt(argv, 'r:t:s:p:')
for opt, arg in opts:
  if opt == '-r':
    region = arg
  elif opt == '-t':
    table = arg  
  elif opt == '-s':
    source = arg  
  elif opt == '-p':
    process_count = int(arg)
"""
Make sure that all command line parameters are defined.
"""
if all([region, table, source, process_count]) == False:
  print('usage:')
  print('DDBImport.py -r <region_name> -t <table_name> -s <source_file> -p <process_count>')
else:
  """
  Create a queue to distribute the work.
  """
  queue = multiprocessing.Queue()
  """
  Launch worker processes to do the work. The worker processes receives data from a
  queue.
  """
  workers = []
  for i in range(process_count):
    p = multiprocessing.Process(target=ddbImportWorker, args=(i, region, table, queue))
    workers.append(p)
    p.start()
  """
  Open the data file (JSON file) for read. Push all items in the data file into the
  queue. Each worker process will poll the queue to do the work.
  """
  with open(source) as f:
    for line in f:
      queue.put(json.loads(line))
  """
  Wait for worker processes to exit, then the main thread exits.
  """
  for p in workers:
    p.join()
  print("All done.")