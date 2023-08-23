## Go S3 bucket access utility
Simple go program that lists the contents of a specified S3 bucket.

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

