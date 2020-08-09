This project provides an easy way to import data from JSON files into DynamoDB table, or export data from DynamoDB table into JSON files. It also provides a data generation utility for quick testing.

## Installation

DDBImportExport requires Python 3, Virtual Environments (venv), and boto3.

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

With a small table (at GB scale), it is safe to use 1 process per vCPU core. If you have an EC2 instance with 4 vCPU cores, it is OK to set the process count to 4. However, it is important that you have sufficient provisioined RCU on the table, and specify sufficient max capacity for the export with the -c option. In general, a single process can achieve over 3200 RCU, which is approximately 25 MB/s. With 4 processes, you can achieve approximately 13000 RCU or 100 MB/s. 

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

## Performance and Cost Considerations for DDBExport

When exporting a DynamoDB table at TB scale, you might want to run DDBExport on an EC2 instance with both good network performance and good disk I/O capacity. The I3 instance family becomes a great choice for such use case. The following test results are done with a DynamoDB table with 6.8 TB data. There are over 37 million items in the table, with each item being around 200 KB. The following tests are done on i3.8xlarge, i3.16xlarge and i3en.24xlarge with Amazon Linux 2. A RAID0 device is created with all the instance-store volumes to provide the best disk I/O capacity. 

On i3.8xlarge, create a RAID0 device with 4 instance-store volumes:

~~~~
sudo mdadm --create /dev/md0 --level=0 --name=RAID0 --raid-devices=4 /dev/nvme0n1 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1
~~~~

On i3.16xlarge, create a RAID0 device with 8 instance-store volumes:

~~~~
sudo mdadm --create /dev/md0 --level=0 --name=RAID0 --raid-devices=8 /dev/nvme0n1 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1 /dev/nvme5n1 /dev/nvme6n1 /dev/nvme7n1
~~~~

On i3en.24xlarge, create a RAID0 device with 8 instance-store volumes:

~~~~
sudo mdadm --create /dev/md0 --level=0 --name=RAID0 --raid-devices=8 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1 /dev/nvme5n1 /dev/nvme6n1 /dev/nvme7n1 /dev/nvme8n1 
~~~~

After that, create EXT4 file system without lazy initialization, mount the RAID0 device for writing. On i3.8xlarge, we use 32 processes. On i3.16xlarge, we use 64 processes. On i3en.24xlarge, we use both 64 and 96 processes.

~~~~
# Create EXT4 file system without lazy initialization
sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 /dev/md0
# Mount the RAID0 device and change the ownership to ec2-user
sudo mkdir /data
sudo mount /dev/md0 /data
sudo chown -R ec2-user:ec2-user /data
# Time the DDBExport process
cd /data
time python ~/DDBImportExport/DDBExport.py -r <region> -t <table> -p <processes> -c <capacity> -s 1024 -d <destination>
~~~~

The following table summaries the tests peformed with DDBExport.

| ID | RCU | Instance | vCPU | Memory | SSD Disks | Network | Processes | Output |
|---|---|---|---|---|---|---|---|---|
| 1 | 112000 | i3.8xlarge | 32 | 244 GB | 4 x 1900 GB | 10 Gbps | 32 | HD |
| 2 | 112000 | i3.8xlarge | 32 | 244 GB | 4 x 1900 GB | 10 Gbps | 32 | S3 |
| 3 | 192000 | i3.16xlarge | 64 | 488 GB | 8 x 1900 GB | 25 Gbps | 64 | HD |
| 4 | 192000 | i3.16xlarge | 64 | 488 GB | 8 x 1900 GB | 25 Gbps | 64 | S3 |
| 5 | 192000 | i3en.24xlarge | 96 | 768 GB | 8 x 7500 | 100 Gbps | 64 | HD |
| 6 | 192000 | i3en.24xlarge | 96 | 768 GB | 8 x 7500 | 100 Gbps | 64 | S3 |
| 7 | 192000 | i3en.24xlarge | 96 | 768 GB | 8 x 7500 | 100 Gbps | 96 | HD |
| 8 | 192000 | i3en.24xlarge | 96 | 768 GB | 8 x 7500 | 100 Gbps | 96 | S3 |

As a comparison, we use Data Pipeline with the "Export DynamoDB table to S3" template to perform the same export. Data Pipeline launches an EMR cluster to do the work, and automatically adjust the number of core nodes to match the provisioned RCU on the table. By default, the m3.xlarge instance type is used, with up to 8 containers on each core node. The following table shows the time needed to perform the export with Data Pipeline.

| ID | RCU | Instance | vCPU | Memory | Nodes | Containers | Output |
|---|---|---|---|---|---|---|---|
| 9 | 112000 | m3.xlarge | 4 | 15 GB | 1 + 94 | 749 | S3 |
| 10 | 192000 | m3.xlarge | 4 | 15 GB | 1 + 160 |  1277 | S3 |

The following table summarizes the execution time and execution cost for the above-described tests, using on-demand pricing in the us-east-1 region. 

| ID | EC2/EMR Price | RCU Price | Consumed RCU | Time | Cost |
|---|---|---|---|---|---|
| 1 | $2.496 / hour | $14.56 / hour | - | - | - |
| 2 | $2.496 / hour | $14.56 / hour | 62000 | 239 minutes | $67.94 |
| 3 | $4.992 / hour | $24.96 / hour | - | - | - |
| 4 | $4.992 / hour | $24.96 / hour | 100000 | 159 minutes | $79.37 |
| 5 | $10.848 / hour | $24.96 / hour | - | - | - |
| 6 | $10.848 / hour | $24.96 / hour | 140000 | 110 minutes | $65.64 |
| 7 | $10.848 / hour | $24.96 / hour | - | - | - |
| 8 | $10.848 / hour | $24.96 / hour | 192000 | 80 minutes | $67.94 |
| 9 | $31.588 / hour | $14.56 / hour | 112000 | 136 minutes | $104.60 |
| 10 | $53.530 / hour | $24.96 / hour | 192000 | 84 minutes | $109.89 |

It should be noted that in test 2, 4 and 6, a significant portion of the provisioned RCU is not used. With S3 as the output destination, each sub-process alternates between DynamoDB Scan and S3 PutObject operations, both producing a significant pressure on the network. When the network is not fast enough, this alternative workload pattern slows down the export process. As shown in test 8, with sufficient network bandwidth and increased concurrency, it is possible to fully utilize 192000 provisioned RCU on a single node, even with output on S3. 

Comparing tests with output to S3 and tests with output to HD, tests with output to HD achieve x% to y% speed-up. Considering the cost of the provisioned RCU is significant, if the RAID0 device is sufficient to hold all the exported data (the RAID0 device on i3en.24xlarge provides 55 TB storage), it would be more cost-effective to perform the export with a multi-step approach, as below:

- Use DDBExport to export to HD.
- Reduce the provisioned RCU on the table.
- Sync the exported data from HD to S3 with the [aws s3 sync](https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html) command.

Comparing test 8 with test 10, DDBExport achieves x% speed-up and y% cost reduction, as compared to Data Pipeline. Also, Data Pipeline is a much more complicate solution, with multiple AWS services involved, and a large number of nodes in a cluster. DDBExport achieves this with a single command line on a single EC2 instance. As such, DDBExport is an ideal tool to export DynamoDB tables at TB scale, with both cost and deadline constraints. 

## Performance and Cost Considerations for DDBImport

To be added when the DDBImport module is improved to support both HD and S3 input source, as well as multiple input files.