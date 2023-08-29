package main

import (
	"context"
	"fmt"
	"github.com/alicel/go-s3-bucket-access/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"strings"
)

func createS3Client(ac *config.AccessorConfig) (*s3.Client, error) {
	var cfg aws.Config
	var err error

	if ac.AccessKey != "" && ac.SecretKey != "" {
		cfg = aws.Config{
			Region:      ac.Region,
			Credentials: credentials.NewStaticCredentialsProvider(ac.AccessKey, ac.SecretKey, ""),
		}
	} else if ac.ProfileName != "" {
		cfg, err = s3config.LoadDefaultConfig(context.Background(),
			s3config.WithSharedConfigProfile(ac.ProfileName),
			s3config.WithRegion(ac.Region),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to load s3 client from profile %v for region %v, %v", ac.ProfileName, ac.Region, err)
		}
	} else {
		return nil, fmt.Errorf("neither static credentials nor profile were specified: please provide either option")
	}

	return s3.NewFromConfig(cfg), nil
}

func persistObjectToBucket(s3uploader *s3manager.Uploader, bucketName string, objectKey string, objectContent string) error {

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(objectContent),
	}

	uploadOutput, err := s3uploader.Upload(context.Background(), uploadInput)

	if err != nil {
		fmt.Errorf("couldn't upload file to %v with key %v due to: %v\n", bucketName, objectKey, err)
	}

	fmt.Printf("Uploaded file to location: %v\n", uploadOutput.Location)
	return nil
}

// TODO this is probably not necessary and may be removed
//func retrieveObjectContentFromBucket(s3Client *s3.Client, bucketName string, objectKey string) (string, error) {
//	result, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
//		Bucket: aws.String(bucketName),
//		Key:    aws.String(objectKey),
//	})
//	if err != nil {
//		return "", fmt.Errorf("object with key %v could not be retrieved from bucket %v due to: %v", objectKey, bucketName, err)
//	}
//	defer result.Body.Close()
//
//	body, err := io.ReadAll(result.Body)
//	if err != nil {
//		return "", fmt.Errorf("object body for key %v could not be read due to: %v", objectKey, err)
//	}
//
//	return string(body), nil
//}
