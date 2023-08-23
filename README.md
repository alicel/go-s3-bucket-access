## Go S3 bucket access utility
Simple go program that reads a bucket containing snapshots from a Cassandra cluster for a data migration and creates 
the following descriptors in the same bucket:
- One descriptor per SSTable
- A global descriptor containing some global state (total number of SSTables, total data size, ...)

This program can also write state to a specified k8s config map: at the moment, this consists of the prefix common to 
all SSTable descriptor S3 keys and the total number of SSTables. This is optional and only happens if a config map is specified. 

### Building it
To build it, run`go build` from the project root directory. You may need to pull in some dependencies using `go get`.

### Running it

Requires an AWS account with at least one S3 bucket.

Credentials are required and can be passed in one of these two ways:
- Explicitly specified as command-line parameters:
    - Specify both `accessKey` and `secretKey`.
- Loaded from an existing AWS profile
    - Specify `profileName`.
    - If using AWS SSO, ensure that you are logged into the profile that you want to use (`aws sso login --profile=profileName`)

You will also need to provide:
- The AWS region in which the bucket is, as `region`
- The name of the bucket, as `bucketName`
- The identifier of your migration, as `migrationId`

To run the utility with static credentials:
`./go-s3-bucket-access -accessKey <my_ak> -secretKey <my_sk> -region <my_reg> -bucketName <my_bn> -migrationId <my_migration_id>`

To run the utility using a profile:
`./go-s3-bucket-access -profileName <my_pn> -region <my_reg> -bucketName <my_bn> -migrationId <my_migration_id>`

### Building it as a Docker image and running it as a container

To build the image (after cloning this repo locally):
`docker build --no-cache --tag go-s3-bucket-access .`

To run it as a container:
`docker run go-s3-bucket-access -accessKey <my_ak> -secretKey <my_sk> --region <my_reg> -bucketName <my_bn> -migrationId <my_migration_id>`

#### Note

When running it as a container, passing a profile name is not supported at the moment: you will need to specify a valid AWS accessKey / secretKey pair. 