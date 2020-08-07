This project provides an easy way to import data from JSON files into DynamoDB table, or export data from DynamoDB table into JSON files. It also provides a data generation utility for quick testing.

## Installation

On a newly launched EC2 instance with Amazon Linux 2, install DDBImportExport with the following commands:

~~~~
sudo yum install git python3 -y
python3 -m venv boto3
source boto3/bin/activate
pip install pip --upgrade
pip install boto3
git clone https://github.com/qyjohn/DDBImportExport
cd DDBImportExport
~~~~

On a newly launched EC2 instance with Ubuntu 18.04, install DDBImportExport with the following commands:

~~~~
sudo apt update
sudo apt install python3 python3-venv git -y
python3 -m venv boto3
source boto3/bin/activate
pip install pip --upgrade
pip install boto3
git clone https://github.com/qyjohn/DDBImportExport
cd DDBImportExport
~~~~

## DDBImport

DDBImport is a python script to import from JSON file into DynamoDB table. The following parameters are required:

| parameter  |  description |
|---|---|
| -r | The name of the AWS region, such as us-east-1. |
| -t | The name of the DynamoDB table. |
| -s | The name of the source file. |
| -p | The number of sub-processes (threads) to use. |

Usage:

~~~~
python DDBImport.py -r <region_name> -t <table_name> -s <source_file> -p <process_count>
~~~~

Example:

~~~~
python DDBImport.py -r us-east-1 -t TestTable -s test.json -p 8
~~~~
  
The script launches multiple processes to do the work. The processes poll from a common queue for data to write. When the queue is empty, the processes continues to poll the queue for another 60 seconds to make sure it does not miss anything. 

It is safe to use 1 process per vCPU core. If you have an EC2 instance with 4 vCPU cores, it is OK to set the process count to 4. The BatchWriteItem API is used to perform the import. Depending on the size of the items, each process can consume approximately 1000 WCU during the import. 

Tested on an EC2 instance with the c3.8xlarge instance type. The data set contains 10,000,000 items, with each item being approximately 170 bytes. The size of the JSON file is 1.7 GB. The DynamoDB table has 40,000 provisioned WCU. Perform the import with 32 threads, and the import is completed in 7 minutes. The peak consumed WCU is approximately 32,000 (average value over a 1-minute period).

It is recommended that you use either a fixed provisioned WCU or an on-demand table for the import. The import creates a short burst traffic, which is not friendly for the DynamoDB auto scaling feature. If you use provisioned capacity, remember that each process requires approximately 1000 WCU. If you use 8 processes to do the import, you need 8000 provisioned WCU on the table.

## DDBExport

DDBExport is a python script to export data from DynamoDB table into JSON file. The following parameters are required:

| parameter  |  description |
|---|---|
| -r | The name of the AWS region, such as us-east-1. |
| -t | The name of the DynamoDB table. |
| -p | The number of sub-processes (threads) to use. |
| -c | The maximum amount of read capacity units (RCU) to use. |
| -s | The maximum size of each individual output file, in MB. |
| -d | The output destination. Supports both local folder and S3. |

Usage:

~~~~
python DDBExport.py -r <region> -t <table> -p <processes> -c <capacity> -s <size> -d <destination>
~~~~

Example:

~~~~
python DDBExport.py -r us-east-1 -t TestTable1 -p 8 -c 1000 -s 1024 -d /data
python DDBExport.py -r us-west-2 -t TestTable2 -p 8 -c 2000 -s 2048 -d s3://bucket/prefix/
~~~~

It is safe to use 1 process per vCPU core. If you have an EC2 instance with 4 vCPU cores, it is OK to set the process count to 4. However, it is important that you have sufficient provisioined RCU on the table, and specify sufficient max capacity for the export with the -c option. In general, a single process can achieve over 3200 RCU, which is approximately 25 MB/s. With 4 processes, you can achieve approximately 13000 RCU or 100 MB/s.

Depending on the number of sub-processes you use and the maximum size of the output file, DDBExport will create multiple JSON files in the output destination. The name of the JSON files will be TableName-WorkerID-FileNumber.json. 

It is important that you have sufficient free space in the output destination. When using S3 as the output destination, your current folder needs to have sufficient free space to hold the intermediate data, which is the number of sub-processes times the size of each individual output file. For example, if the number of sub-processes is 8, and the size of each individual output file is 1024 MB, then you will need 8 x 1024 MB = 8 GB free space in the current directory. 

## Data Format

DDBImport/DDBExport uses regular JSON data format, one item per line, as shown in the example below. This allows the data to be used directly in other use cases, for example, AWS Glue and AWS Athena. 

~~~~
{"hash": "ABC", "range": "123", "val_1": "ABCD"}
{"hash": "BCD", "range": "234", "val_2": 1234}
{"hash": "CDE", "range": "345", "val_1": "ABCD", "val_2": 1234}
~~~~

The JSON data must include the primary key of your DynamoDB table. In the above-mentioned example, attribute "hash" is the hash key and attribute "range" is the range key.

We also provide a data generation utility GenerateTestData.py for testing purposes. 

Usage:

~~~~
python GenerateTestData.py -c <item_count> -f <output_file>
~~~~
  
Example:

~~~~
python GenerateTestData.py -c 1000000 -f test.json
python DDBImport.py -r us-east-1 -t TestTable -s test.json -p 8
~~~~

## Performance Considerations

When exporting a DynamoDB table at TB scale, you might want to run DDBExport on an EC2 instance with instance-store volumes. The I3 instance family becomes a great choice for such use case. The following test results are done with a DynamoDB table with 6.78 TB data. There are XXX items in the table, with each item being 399.2 KB. 

| instance type  | vCPU | Memory | Network |
|---|---|---|---|
| i3.8xlarge | 32 | 244 GB | 10 Gbps |
| i3.16xlarge | 64 | 488 GB | 25 Gbps |


## Others

You should also be aware of the following dynamodump project:

https://github.com/bchew/dynamodump
