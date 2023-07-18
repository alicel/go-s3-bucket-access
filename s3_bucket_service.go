package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"os"
)

type BucketObject struct {
	key string
	size int64
}

func getS3Client(accessKey string, secretKey string, profileName string, region string) (*s3.Client, error) {
	var cfg aws.Config
	var err error

	if region == "" {
		return nil, fmt.Errorf("missing mandatory AWS region parameter")
	}

	if (accessKey != "" && secretKey == "") || (accessKey == "" && secretKey != "") {
		return nil, fmt.Errorf("incomplete AWS credentials: please specify both Access Key and Secret Key")
	}

	if accessKey != "" && secretKey != "" {
		cfg = aws.Config{
			Region:			region,
			Credentials:	credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		}
	} else if profileName != "" {
		cfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithSharedConfigProfile(profileName),
			config.WithRegion(region),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to load s3 client from profile %v for region %v, %v", profileName, region, err)
		}
	} else {
		return nil, fmt.Errorf("neither static credentials nor profile were specified: please provide either option")
	}

	return s3.NewFromConfig(cfg), nil
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func findObjectsInBucket(s3Client *s3.Client, bucketName string) ([]BucketObject, error) {

	if bucketName == "" {
		return nil, fmt.Errorf("missing mandatory bucket name parameter")
	}
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, err
	}

	var contentList []BucketObject
	for _, object := range output.Contents {
		bucketObject := BucketObject{
			key:  aws.ToString(object.Key),
			size: object.Size,
		}
		contentList = append(contentList, bucketObject)
	}
	return contentList, nil
}

func listBucketsForAccount(s3Client *s3.Client, maxBuckets int) ([]string, error) {

	result, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("couldn't list buckets for this account: %v", err)

	}

	bucketNames := make([]string, maxBuckets)
	if maxBuckets > len(result.Buckets) {
		maxBuckets = len(result.Buckets)
	}
	for _, bucket := range result.Buckets[:maxBuckets] {
		bucketNames = append(bucketNames, *bucket.Name)
	}

	return bucketNames, nil
}