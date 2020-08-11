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
import queue
#from multiprocessing import Queue
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
Each ddbImportWorker is a sub-process to read data and write to DynamoDB. 
The QoSCounter is used for QoS control.
"""        
def ddbImportWorker(id, region, table, queue, source_type, counter):
  """
  We create one DynamoDB client per worker process. This is because boto3 session 
  is not thread safe. 
  """
  session  = boto3.session.Session()
  dynamodb = session.resource('dynamodb', region_name = region)
  ddb_table   = dynamodb.Table(table)
  with ddb_table.batch_writer() as batch:
    if source_type == 'DIR':
      """
      When the source_type is DIR, each record in the queue is a filename.
      """
      has_more_work = True
      while has_more_work:
        try:
          file = queue.get(timeout=3)
          with open(file) as f:
            for line in f:
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
        except Exception as e:
          has_more_work = False  
          print(str(e))
          sys.exit()
    elif source_type == 'FILE':
      """
      When the source_type is FILE, each record in the queue is an item.
      """
      has_more_work = True
      while has_more_work:
        try:
          """
          Before doing any work, wait for QoSCounter to be greater than zero. 
          """
          while counter.value() <= 0:
            time.sleep(1)
          line = queue.get(timeout=3)
          size = len(line)
          item = json.loads(line)
          batch.put_item(Item=item)
          """
          Consume (size/1024) WCU from the counter. This only an estimation.
          """
          counter.consume(math.ceil(size/1024))
        except Exception as e:
          has_more_work = False  
          print(str(e))
          sys.exit()
  """
  Keep on polling the queue for items to work on. 
  Use BatchWriteItem to write items in batches into the DynamoDB table. In boto3, 
  the DynamoDB.Table.batch_writer() automatically handles buffering and sending 
  items in batches, and automatically handles any unprocessed items and resends 
  them when needed. 
  with ddb_table.batch_writer() as batch:
    work = 1
    while work == 1:
      try:
        item = queue.get(timeout=60)
        batch.put_item(Item=item)
      except Queue.Empty:
        work = 0  
  """


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
  Check if the input source is a file or a folder.
  """
  source_type = 'DIR'
  if os.path.exists(source):
    if os.path.isfile(source):
      source_type = 'FILE'
      if not source.endswith('.json'):
        print('Input source ' + source + ' does not have the .json filename extension.')
        sys.exit()
    elif os.path.isdir(source):
      source_type = 'DIR'
    else:
      print('Input source ' + source + ' is not valid.')
      sys.exit()
  else:
    print('Input source ' + source + ' does not exist.')
    sys.exit()
  """
  Create a queue to distribute the work.
  """
  queue = multiprocessing.Queue()
  if source_type == 'DIR':
    """
    Push all the JSON files under the folder into the Queue.
    """
    files = [y for x in os.walk(os.path.abspath(source)) for y in glob(os.path.join(x[0], '*.json'))]
    for file in files:
      queue.put(file)
  else:
    """
    Open the data file (JSON file) for read. Push all items in the data file into the
    queue. Each worker process will poll the queue to do the work.
    """
    with open(source) as f:
      for line in f:
        queue.put(line)
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
    p = multiprocessing.Process(target=ddbImportWorker, args=(i, region, table, queue, source_type, counter))
    workers.append(p)
    p.start()
  """
  Wait for worker processes to exit, then the main thread exits.
  """
  for p in workers:
    p.join()
  qos.terminate()
  print("All done.")