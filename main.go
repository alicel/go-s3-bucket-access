package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	accessKeyPtr := flag.String("accessKey", "", "A valid access key for your AWS account. Requires a Secret Key as well")
	secretKeyPtr := flag.String("secretKey", "", "The secret key for the access key that you specified. Requires an Access Key as well")
	profileName := flag.String("profileName", "", "The profile name to load credentials for. Only used if Access Key and Secret Key were not specified")
	region := flag.String("region", "", "The AWS region where your S3 bucket is")
	bucketName := flag.String("bucketName", "", "The name of your S3 bucket")

	flag.Parse()

	fmt.Printf("Provided parameters: AK <%v>, SK <%v>, Profile <%v>, Region <%v>, Bucket <%v>\n", *accessKeyPtr, *secretKeyPtr, *profileName, *region, *bucketName)

	s3Client, err := getS3Client(*accessKeyPtr, *secretKeyPtr, *profileName, *region)

	if err != nil {
		fmt.Printf("Error while creating the S3 Client: %v\n", err)
		os.Exit(2)
	}

	bucketObjects, err := findObjectsInBucket(s3Client, *bucketName)
	if err != nil {
		fmt.Printf("Error while retrieving the bucket contents: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("Found %v objects in bucket %v\n", len(bucketObjects), bucketName)
	for i, bucketObject := range bucketObjects {
		fmt.Printf("%v: %v (%v)\n", i, bucketObject.key, formatBucketSize(bucketObject.size))
	}

}

func formatBucketSize(sizeInBytes int64) string {
	const oneKB = float64(1024)
	const oneMB = oneKB * float64(1024)
	const oneGB = oneMB * float64(1024)
	const oneTB = oneGB * float64(1024)


	floatSize :=float64(sizeInBytes)
	switch {
	case floatSize < oneMB:
		size := floatSize / oneKB
		return fmt.Sprintf("%.2f KB", size)
	case floatSize < oneGB:
		size := floatSize / oneMB
		return fmt.Sprintf("%.2f MB", size)
	case floatSize < oneGB*1024:
		size := floatSize / oneGB
		return fmt.Sprintf("%.2f GB", size)
	default:
		size := floatSize / oneTB
		return fmt.Sprintf("%.2f TB", size)
	}

}