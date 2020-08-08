"""
This is a python script to export data from DynamoDB table into JSON files. 

Usage:
  python DDBExport.py -r <region> -t <table> -p <processes> -c <capacity> -s <size> -d <destination>

Example:
  python DDBExport.py -r us-east-1 -t TestTable1 -p 8 -c 1000 -s 1024 -d /data
  python DDBExport.py -r us-west-2 -t TestTable2 -p 8 -c 2000 -s 2048 -d s3://bucket/prefix/
  
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
import os

"""
QoSCounter is a LeakyBucket QoS algorithm. Each sub-process can not do any Scan 
unless the QoSCounter is greater than 0. After a sub-process performs a Scan, it
must deduct the consumed RCU from the QoSCounter by calling the consume() method. 
The main process needs to start a separate process to call the refill() method
at 1 Hz to refill the LeakyBucket. 
"""
class QoSCounter(object):
    def __init__(self, value=0):
        # RawValue because we don't need it to create a Lock:
        self.capacity   = multiprocessing.RawValue('i', value)
        self.refillRate = multiprocessing.RawValue('i', value)
        self.lock       = multiprocessing.Lock()

    def consume(self, value=0):
        with self.lock:
            self.capacity.value -= value

    def refill(self):
        with self.lock:
            if self.capacity.value < 0:
                self.capacity.value += self.refillRate.value
            else:
                self.capacity.value = self.refillRate.value

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
This is a method to convert Decimal into integer or float.
"""
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
      if int(obj) == obj:
        return int(obj)
      else:
        return float(obj)
    raise TypeError
   
"""
Each ddbExportWorker is a sub-process to Scan and export one of the segments. 
The QoSCounter is used for QoS control.
"""   
def ddbExportWorker(workerId, region, table, total_segments, counter, destination, size, isS3, s3Bucket, s3Prefix):
  """
  We create one DynamoDB client per worker process. This is because boto3 session 
  is not thread safe. If the destination is on S3, then we create an S3 client as 
  well.
  """
  session = boto3.session.Session()
  dynamodb    = session.resource('dynamodb', region_name = region)
  ddb_table   = dynamodb.Table(table)
  if isS3:
    s3 = session.resource('s3', region_name = region)
  
  """
  Output filename is table-workerId-fileId.json.
  """
  fileId = 0
  if isS3:
    filename = str(table) + '-' + "{:04d}".format(workerId) + '-' + "{:04d}".format(fileId) + '.json'
  else:
    filename = destination + str(table) + '-' + "{:04d}".format(workerId) + '-' + "{:04d}".format(fileId) + '.json'
  out=open(filename, 'w')
  """
  Keep on scanning the segment until the end of the segment. Before each scan, wait for QoSCounter 
  to be greater than zero. 
  """
  while counter.value() <= 0:
    time.sleep(1)
  response = ddb_table.scan(TotalSegments=total_segments, Segment=workerId, ReturnConsumedCapacity='TOTAL')  
  counter.consume(int(response['ConsumedCapacity']['CapacityUnits']))
  for item in response['Items']:
    out.write(json.dumps(item, default=decimal_default) + '\n')
  scans = 1
  while 'LastEvaluatedKey' in response:
    while counter.value() <= 0:
      time.sleep(1)
    response = ddb_table.scan(TotalSegments=total_segments, Segment=workerId, ExclusiveStartKey=response['LastEvaluatedKey'], ReturnConsumedCapacity='TOTAL')
    counter.consume(int(response['ConsumedCapacity']['CapacityUnits']))
    for item in response['Items']:
      out.write(json.dumps(item, default=decimal_default) + '\n')
    scans = scans + 1
    """
    Create a new file when the file size approaches the size limit.
    """
    if scans == size:
      out.close()
      fileId = fileId + 1
      if isS3:
        """
        Stage this file to S3, delete it from local disk, then create the next filename
        """
        if s3Prefix is None:
          s3.meta.client.upload_file(filename, s3Bucket, filename)
        else:
          s3.meta.client.upload_file(filename, s3Bucket, s3Prefix + filename)
        os.remove(filename)
        filename = str(table) + '-' + "{:04d}".format(workerId) + '-' + "{:04d}".format(fileId) + '.json'
      else:
        filename = destination + str(table) + '-' + "{:04d}".format(workerId) + '-' + "{:04d}".format(fileId) + '.json'
      out=open(filename, 'w')
      scans = 0
  """
  Now we are done with scanning this segment. If the destination is S3, then we need
  to stage the last file to S3 and delete from local disk.
  """
  out.close()
  if isS3:
    if s3Prefix is None:
      s3.meta.client.upload_file(filename, s3Bucket, filename)
    else:
      s3.meta.client.upload_file(filename, s3Bucket, s3Prefix + filename)
    os.remove(filename)


"""
The main program starts here.
At the beginning, nothing is defined. Enforce user-supplied values.
"""
region = None
table  = None
process_count = None
rcu    = None
size   = 1024
destination  = None
isS3   = False
s3Bucket = None
s3Prefix = None
"""
Obtain the AWS region, table name, and the number of worker processes from command line.
"""
argv = sys.argv[1:]
opts, args = getopt.getopt(argv, 'r:t:p:c:s:d:')
for opt, arg in opts:
  if opt == '-r':
    region = arg
  elif opt == '-t':
    table = arg  
  elif opt == '-d':
    destination = arg  
    if destination.startswith('s3://'):
      isS3 = True
      destination = destination[5:]
      """
      Dealing with S3 bucket name and prefix
      """
      if destination.endswith('/'):
        destination = destination[:-1]
      pos = destination.find('/')
      if pos != -1:
        s3Bucket = destination[:pos]
        s3Prefix = destination[pos+1:] + '/'
      else:
        s3Bucket = destination
        s3Prefix = None
    else:
      if not destination.endswith('/'):
        destination = destination + '/'
      if not os.path.exists(destination):
        os.mkdir(destination)
  elif opt == '-p':
    process_count = int(arg)
  elif opt == '-c':
    rcu = int(arg)
  elif opt == '-s':
    size = int(arg)
"""
Make sure that all command line parameters are defined.
"""
if all([region, table, process_count, rcu, destination, size]) == False:
  print('usage:')
  print('python DDBExport.py -r <region> -t <table> -p <processes> -c <capacity> -s <size> -d <destination>')
else:
  """
  Make sure the DynamoDB table exists and has the desired level of RCU. 
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
      print('The DynamoDB table has provisioned RCU: ' + str(response.provisioned_throughput['ReadCapacityUnits']))
      if response.provisioned_throughput['ReadCapacityUnits'] < rcu:
        print('The provisioned RCU is smaller than the desired capacity (' + str(rcu) + ') for DDBExport.')
        sys.exit()
    else:
      print('The DynamoDB table is using on-demand capacity.')
  except Exception as e:
    print(str(e))
    sys.exit()
  """
  Setup the QoSCounter. 
  """
  counter = QoSCounter(rcu)
  qos = multiprocessing.Process(target=qosRefillThread, args=(counter, ))
  qos.start()
  """
  Launch worker processes to do the work. 
  """
  workers = []
  for i in range(process_count):
    p = multiprocessing.Process(target=ddbExportWorker, args=(i, region, table, process_count, counter, destination, size, isS3, s3Bucket, s3Prefix))
    workers.append(p)
    p.start()
  """
  Wait for worker processes to exit, then the main thread exits.
  """
  for p in workers:
    p.join()
  qos.terminate()
  print("All done.")