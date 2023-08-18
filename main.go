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
	migrationId := flag.String("migrationId", "", "The identifier of this migration")
	// TODO these will be needed later on
	//keyPrefix := flag.String("keyPath", "", "The prefix to filter the returned keys")
	//maxKeys := flag.Int("maxKeys", 1000, "The max number of keys to return")

	flag.Parse()

	fmt.Printf("Provided parameters: AK <%v>, SK <%v>, Profile <%v>, Region <%v>, Bucket <%v>\n", *accessKeyPtr, *secretKeyPtr, *profileName, *region, *bucketName)

	migrationBucketAccessor, err := NewMigrationBucketAccessor(*accessKeyPtr, *secretKeyPtr, *profileName, *region, *bucketName, *migrationId)
	if err != nil {
		fmt.Printf("Error while creating the bucket accessor: %v\n", err)
		os.Exit(2)
	}

	err = migrationBucketAccessor.InitSSTableDescriptors()
	if err != nil {
		fmt.Printf("Error while creating the SSTable descriptors: %v\n", err)
		os.Exit(2)
	}
}

func formatBucketSize(sizeInBytes int64) string {
	const oneKB = float64(1024)
	const oneMB = oneKB * float64(1024)
	const oneGB = oneMB * float64(1024)
	const oneTB = oneGB * float64(1024)

	floatSize := float64(sizeInBytes)
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
