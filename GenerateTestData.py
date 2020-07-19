"""
This is a python script to load data from JSON file into DynamoDB table. 

Usage:
  python GenerateTestData.py -c <item_count> -f <output_file>
  
Example:
  python GenerateTestData.py -c 1000000 -f test.json
"""

#!/usr/bin/python
import sys
import uuid
import random
import getopt

"""
At the beginning, nothing is defined. Enforce user-supplied values.
"""
total = None
file  = None
"""
Obtain the total number of items and the output file name from command line.
"""
argv = sys.argv[1:]
opts, args = getopt.getopt(argv, 'c:f:')
for opt, arg in opts:
  if opt == '-c':
    total = int(arg)
  elif opt == '-f':
    file = arg  
"""
Make sure that all command line parameters are defined.
"""
if all([total, file]) == False:
  print('usage:')
  print('GenerateTestData.py -c <item_count> -f <output_file>')
else:
  out=open(file, 'w')
  out.write('[')
  """
  Write n-1 items first, with "," at the end of each line.
  """
  for i in range(total-1):
    hash  = str(uuid.uuid4())
    range = str(uuid.uuid4())
    val_1 = random.randrange(2147483647)
    val_2 = str(uuid.uuid4())
    out.write('{"hash": "%s", "range": "%s", "val_1": %d, "val_2": "%s"},\n' % (hash, range, val_1, val_2))
  """
  Write the last item.
  """
  hash  = str(uuid.uuid4())
  range = str(uuid.uuid4())
  val_1 = random.randrange(2147483647)
  val_2 = str(uuid.uuid4())
  out.write('{"hash": "%s", "range": "%s", "val_1": %d, "val_2": "%s"}]' % (hash, range, val_1, val_2))
