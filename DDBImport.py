"""
This is a python script to load data from JSON file into DynamoDB table. 

Usage:
  python DDBImport.py -r <region> -t <table> -s <source> -p <processes> -c <capacity>

Example:
  python DDBImport.py -r us-east-1 -t TestTable -s test.json -p 8 -c 1000
  python DDBImport.py -r us-east-1 -t TestTable -s data/ -p 8 -c 1000
  
The script launches multiple processes to do the work. The processes poll from a
common queue for data to write. 

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
import os
import sys
import json
import time
import boto3
import math
import multiprocessing
import getopt
from glob import glob

"""
QoSCounter is a LeakyBucket QoS algorithm. Each sub-process can not do any Scan 
unless the QoSCounter is greater than 0. After a sub-process performs a Scan, it
must deduct the consumed RCU from the QoSCounter by calling the consume() method. 
The main process needs to start a separate process to call the refill() method
at 1 Hz to refill the LeakyBucket. 
"""
class QoSCounter(object):
    def __init__(self, value=0):
        """
        RawValue because we don't need it to create a Lock:
        """
        self.capacity   = multiprocessing.RawValue('i', value)
        self.refillRate = multiprocessing.RawValue('i', value)
        self.lock       = multiprocessing.Lock()

    def consume(self, value=0):
        with self.lock:
            self.capacity.value -= value

    def refill(self):
        """
        Here we assume unlimit capacity for the LeakyBucket. The underlying assumption
        is unused capacity in the previous second is counted towards burst capacity,
        which can be used in subsequent API calls. 
        """ 
        with self.lock:
            self.capacity.value += self.refillRate.value

    def value(self):
        with self.lock:
            return self.capacity.value

"""
This is a thread to refill the QoSCounter once every second.
"""
def qosRefillThread(counter):
  while True:
      counter.refill()
      time.sleep(1)
      

"""
Write one item.
"""
def writeItem(batch, line, counter):
  """
  Before doing any work, wait for QoSCounter to be greater than zero. 
  """
  while counter.value() <= 0:
    time.sleep(1)
  size = len(line)
  item = json.loads(line)
  batch.put_item(Item=item)
  """
  Consume (size/1024) WCU from the counter. This is only an estimation.
  """
  counter.consume(math.ceil(size/1024))


"""
Each ddbImportWorker is a sub-process to read data and write to DynamoDB. 
The QoSCounter is used for QoS control.
"""        
def ddbImportWorker(workerId, region, table, queue, queue_type, counter):
  worker = "Worker_" + "{:04d}".format(workerId)
  """
  We create one DynamoDB client per worker process. This is because boto3 session 
  is not thread safe. 
  """
  session  = boto3.session.Session()
  dynamodb = session.resource('dynamodb', region_name = region)
  ddb_table   = dynamodb.Table(table)
  with ddb_table.batch_writer() as batch:
    if queue_type == 'FILE':
      """
      When the source_type is FILE, each record in the queue is a filename.
      """
      has_more_work = True
      while has_more_work:
        try:
          file = queue.get(timeout=2)
          print(worker + ' is importing ' + file)
          with open(file) as f:
            for line in f:
              writeItem(batch, line, counter)
        except Exception as e:
          print(str(e))
          has_more_work = False  
          sys.exit()
    elif queue_type == 'LINE':
      """
      When the queue_type is LINE, each record in the queue is an item.
      """
      has_more_work = True
      while has_more_work:
        try:
          line = queue.get(timeout=2)
          writeItem(batch, line, counter)
        except Exception as e:
          print(str(e))
          has_more_work = False  
          sys.exit()

"""
Retrieve all JSON files under the S3 prefix.
"""
def listS3Objects(s3Bucket, s3Prefix):
  results = []
  try:
    """
    Create S3 client to ListObjects
    """
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=s3Bucket, Prefix=s3Prefix, MaxKeys=2)
    if 'Contents' in response:
      for item in response['Contents']:
        if item['Key'].endswith('json'):
          results.append(item['Key'])
    while response['IsTruncated']:
      response = client.list_objects_v2(Bucket=s3Bucket, Prefix=s3Prefix, MaxKeys=2, ContinuationToken=response['NextContinuationToken'])
      if 'Contents' in response:
        for item in response['Contents']:
          if item['Key'].endswith('json'):
            results.append(item['Key'])
  except Exception as e:
    print(str(e))
    sys.exit()
  return results


"""
Retrieve all JSON files under the local path
"""
def listLocalFiles(source):
  files = []
  if os.path.exists(source):
    if os.path.isfile(source):
      if source.endswith('.json'):
        files.append(source)
    elif os.path.isdir(source):
      files = [y for x in os.walk(os.path.abspath(source)) for y in glob(os.path.join(x[0], '*.json'))]
  return files  
  
  
"""
At the beginning, nothing is defined. Enforce user-supplied values.
"""
region = None
table  = None
source = None
wcu    = None
process_count = None
"""
Obtain the AWS region, table name, source file, and the number of worker processes
from command line.
"""
argv = sys.argv[1:]
opts, args = getopt.getopt(argv, 'r:t:s:p:c:')
for opt, arg in opts:
  if opt == '-r':
    region = arg
  elif opt == '-t':
    table = arg  
  elif opt == '-s':
    source = arg  
  elif opt == '-p':
    process_count = int(arg)
  elif opt == '-c':
    wcu = int(arg)
"""
Make sure that all command line parameters are defined.
"""
if all([region, table, source, process_count, wcu]) == False:
  print('usage:')
  print('DDBImport.py -r <region_name> -t <table_name> -s <source> -p <processes> -c capacity')
else:
  """
  Make sure the DynamoDB table exists and has the desired level of WCU. 
  """
  try:
    session = boto3.session.Session()
    client  = session.resource('dynamodb', region_name = region)
    response = client.Table(table)
    print('The DynamoDB table is ' + response.table_status + '.')
    if response.table_status != 'ACTIVE':
      print('The DynamoDB table must be in ACTIVE state to run DDBExport.')
      sys.exit()
    if response.billing_mode_summary is None:
      print('The DynamoDB table has provisioned WCU: ' + str(response.provisioned_throughput['WriteCapacityUnits']))
      if response.provisioned_throughput['WriteCapacityUnits'] < wcu:
        print('The provisioned WCU is smaller than the desired capacity (' + str(wcu) + ') for DDBImport.')
        sys.exit()
    else:
      print('The DynamoDB table is using on-demand capacity.')
  except Exception as e:
    print(str(e))
    sys.exit()
  """
  Create a queue to distribute the work.
  """
  queue = multiprocessing.Queue()
  """
  Check if the input source is an S3 path, a file, or a folder.
  """
  if source.startswith('s3://'):
    """
    Remove s3:// from S3 URI and identify S3 bucket and prefix
    """
    source = source[5:]
    pos = source.find('/')
    s3Bucket = source[:pos]
    s3Prefix = source[pos+1:]
    objects = listS3Objects(s3Bucket, s3Prefix)
    if len(objects) == 0:
      """
      There is no S3 object with .json filename
      """
      print('Can not find any .json file in the S3 path specified.')
      sys.exit()
    elif len(objects) == 1:
      """
      There is only one S3 object with .json filename, need to write to the queue line by line
      """
      queue_type = 'LINE'
    else:
      """
      There are multiple S3 object with .json filename, need to write object key names to the queue
      """
      queue_type = 'S3Object'
      for obj in objects:
        queue.put(obj)
  else:
    """
    Retrieve all JSON files in the source location
    """
    files = listLocalFiles(source)
    if len(files) == 0:
      """
      There is no .json file in the source location
      """
      print('Can not find any .json file in the source location.')
      sys.exit()
    elif len(files) == 1:
      """
      There is only one .json file
      """
      queue_type = 'LINE'
      with open(source) as f:
        for line in f:
          queue.put(line)
    else:
      """
      There are multiple .json files, need to write filenames to the queue
      """
      queue_type = 'FILE'
      for file in files:
        queue.put(file)
  """
  Setup the QoSCounter. 
  """
  counter = QoSCounter(wcu)
  qos = multiprocessing.Process(target=qosRefillThread, args=(counter, ))
  qos.start()
  """
  Launch worker processes to do the work. The worker processes receives data from a
  queue.
  """
  workers = []
  for i in range(process_count):
    p = multiprocessing.Process(target=ddbImportWorker, args=(i, region, table, queue, queue_type, counter))
    workers.append(p)
    p.start()
  """
  Wait for worker processes to exit, then the main thread exits.
  """
  for p in workers:
    p.join()
  qos.terminate()
  print("All done.")