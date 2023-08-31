## Go S3 bucket access utility
Simple go program that reads a bucket containing snapshots from a Cassandra cluster for a data migration and creates 
the following descriptors in the same bucket:
- One descriptor per SSTable
- A single descriptor containing some global state (total number of SSTables, total data size, ...)

This program can also write state to a specified k8s config map: at the moment, this consists of the prefix common to 
all SSTable descriptor S3 keys and the total number of SSTables. This is optional and only happens if a namespace and 
config map are specified.

#### Example SSTable descriptor

S3 key: `mig001/SSTableDescriptors/1/bb-5-bti-1`

Content:
```agsl
{
	"entityNames": {
		"keyspaceName": "baselines",
		"cqlTableName": "iot",
		"ssTableNamePrefix": "bb-5-bti"
	},
	"size": 357421328,
	"keyPath": "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0",
	"componentFileKeys": ["baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-CompressionInfo.db", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-Data.db", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-Digest.crc32", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-Filter.db", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-Partitions.db", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-Rows.db", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-Statistics.db", "baselines_iot_node0/home/automaton/dse_data/cassandra/baselines/iot-3e2d0c602b0311ee80bfa378d3cf1972/snapshots/baselines_backup_node0/bb-5-bti-TOC.txt"]
}
```
#### Example global descriptor

S3 Key: `mig001/globalState-mig001`

Content:
```agsl
{
	"migrationId": "mig001",
	"keyspaceCount": 1,
	"cqlTableCount": 2,
	"ssTableCount": 21,
	"dataSize": 2433884226,
	"ssTableDescriptorKeyPrefix": "mig001/SSTableDescriptors"
}
```

### Parameters

You will need to have an AWS account with at least one S3 bucket.

All parameters are passed as environment variables. These are:
- Credentials. Specify either of these:
  - `MBA_ACCESS_KEY` and `MBA_SECRET_KEY`: AWS access key and secret key to access the S3 bucket
  - `MBA_PROFILE_NAME`: name of an AWS profile to access the bucket (not supported when running this component as a container)
- `MBA_REGION`: AWS region where the S3 bucket is located
- `MBA_BUCKET_NAME`: name of the S3 bucket to access
- `MBA_MIGRATION_ID`: unique identifier for this migration (can be any string)
- To persist state to an existing k8s config map, specify both:
  - `MBA_K8S_CONFIG_MAP_NAMESPACE`: k8s namespace where the config map is located
  - `MBA_K8S_CONFIG_MAP_NAME`: name of the k8s config map

All parameters are required apart from the k8s ones, which can be left out if not using a k8s config map.

### Building and running it as an executable
To build it, run`go build` from the project root directory. You may need to pull in some dependencies using `go get`.

To run the utility, configure the environment variables as explained above and simply run:
`./go-s3-bucket-access`

### Building it as a Docker image and running it as a container

To build the image (after cloning this repo locally):
`docker build --no-cache --tag go-s3-bucket-access .`

To run it as a container, create an environment file with the variables set as explained above and simply run:
`docker run go-s3-bucket-access`

#### Note
When running it as a container, passing a profile name is not supported at the moment: you will need to specify a valid 
AWS accessKey / secretKey pair. 