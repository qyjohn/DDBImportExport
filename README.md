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

You need the following IAM permissions to use this utility:

- dynamodb:DescribeTable
- dynamodb:Scan
- dynamodb:BatchWriteItem
- s3:PutObject

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

It is important that you have sufficient free space in the output destination. When using S3 as the output destination, your current folder needs to have sufficient free space to hold the intermediate data, which is the number of sub-processes times the size of each individual output file. For example, if the number of sub-processes is 8, and the size of each individual output file is 1024 MB, then you will need 8 x 1024 MB = 8 GB free space in the current directory. For the same reason, you will need at least 8 x 1024 MB = 8 GB memory to run the export, because DDBExport holds the intermediate data in memory before flushing to disk. 

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

When exporting a DynamoDB table at TB scale, you might want to run DDBExport on an EC2 instance with both good network performance and good disk I/O capacity. The I3 instance family becomes a great choice for such use case. The following test results are done with a DynamoDB table with 6.78 TB data. There are XXX items in the table, with each item being 399.2 KB. A RAID0 device is created with all the instance-store volumes to provide the best disk I/O capacity. 

On i3.8xlarge:

~~~~
# Create a RAID0 device and create EXT4 file system without lazy initialization
sudo mdadm --create /dev/md0 --level=0 --name=RAID0 --raid-devices=4 /dev/nvme0n1 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1
sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 /dev/md0
# Mount the RAID0 device and change the ownership to ec2-user
sudo mkdir /data
sudo mount /dev/md0 /data
sudo chown -R ec2-user:ec2-user /data
# Time the DDBExport process
cd /data
time python ~/DDBImportExport/DDBExport.py -r us-west-2 -t TestTable2 -p 32 -c 112000 -s 1024 -d s3://bucket/prefix/
~~~~

On i3.16xlarge:

~~~~
# Create a RAID0 device and create EXT4 file system without lazy initialization
sudo mdadm --create /dev/md0 --level=0 --name=RAID0 --raid-devices=8 /dev/nvme0n1 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1 /dev/nvme5n1 /dev/nvme6n1 /dev/nvme7n1
sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 /dev/md0
# Mount the RAID0 device and change the ownership to ec2-user
sudo mkdir /data
sudo mount /dev/md0 /data
sudo chown -R ec2-user:ec2-user /data
# Time the DDBExport process
cd /data
time python ~/DDBImportExport/DDBExport.py -r us-west-2 -t TestTable2 -p 64 -c 192000 -s 1024 -d s3://bucket/prefix/
~~~~

The following table shows the time needed to perform the export with DDBExport.

| RCU | Instance | vCPU | Memory | SSD Disks | Network | Processes | Time |
|---|---|---|---|---|---|---|---|
| 112000 | i3.8xlarge | 32 | 244 GB | 4 x 1900 GB | 10 Gbps | 32 | aaa |
| 192000 | i3.16xlarge | 64 | 488 GB | 8 x 1900 GB | 25 Gbps | 64 |  xxx |

As a comparison, we use Data Pipeline with the "Export DynamoDB table to S3" template to perform the same export. Data Pipeline launches an EMR cluster to do the work, and automatically adjust the number of core nodes to match the provisioned RCU on the table. By default, the m3.xlarge instance type is used, with up to 8 containers on each core node. The following table shows the time needed to perform the export with Data Pipeline.

| RCU | Instance | vCPU | Memory | Core Nodes | Containers | Time |
|---|---|---|---|---|---|---|
| 112000 | m3.xlarge | 4 | 15 GB | 94 | 749 | minutes |
| 192000 | m3.xlarge | 4 | 15 GB | xx |  xxx | minutes |

Now let's do a cost comparision on the above-mentioned approaches (using on-demand pricing in the us-east-1 region):

| Test | Instance | EC2 Price | EMR Price | Total Nodes | Total Time | Total Cost |
|---|---|---|---|---|---|---|
| DDBExport-1 | i3.8xlarge | $2.496 | N/A | 1 | - | - |
| DDBExport-2 | i3.16xlarge | $4.992 | N/A | 1 | - | - |
| Pipeline-1 | m3.xlarge | $0.266 | $0.0665 | 95 | - | - |
| Pipeline-2 | m3.xlarge | $0.266 | $0.0665 | - | - | - |


## Others

You should also be aware of the following dynamodump project:

https://github.com/bchew/dynamodump
