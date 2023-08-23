package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	accessKey := flag.String("accessKey", "", "A valid access key for your AWS account. Requires a Secret Key as well")
	secretKey := flag.String("secretKey", "", "The secret key for the access key that you specified. Requires an Access Key as well")
	profileName := flag.String("profileName", "", "The profile name to load credentials for. Only used if Access Key and Secret Key were not specified")
	region := flag.String("region", "", "The AWS region where your S3 bucket is")
	bucketName := flag.String("bucketName", "", "The name of your S3 bucket")
	migrationId := flag.String("migrationId", "", "The identifier of this migration")
	k8sConfigMapName := flag.String("k8sConfigMapName", "", "The name of the config map to write to (leave empty to disable writing to any config map")

	flag.Parse()

	err := validateInputParameters(accessKey, secretKey, profileName, region, bucketName, k8sConfigMapName, migrationId)
	if err != nil {
		fmt.Printf("Input parameter validation error: %v\n", err)
		os.Exit(2)
	}

	// TODO remove
	fmt.Printf("Provided parameters: AK <%v>, SK <%v>, Profile <%v>, Region <%v>, Bucket <%v>, MigrationId <%v>\n",
		*accessKey, *secretKey, *profileName, *region, *bucketName, *migrationId)

	migrationBucketAccessor, err := NewMigrationBucketAccessor(*accessKey, *secretKey, *profileName, *region, *bucketName, *k8sConfigMapName, *migrationId)
	if err != nil {
		fmt.Printf("Error while creating the bucket accessor: %v\n", err)
		os.Exit(2)
	}

	err = migrationBucketAccessor.InitAndPersistMigrationDescriptors()
	if err != nil {
		fmt.Printf("Error while creating the SSTable and global descriptors: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("Descriptors created and persisted to S3\n")

}

func validateInputParameters(accessKey *string, secretKey *string, profileName *string, region *string,
	bucketName *string, k8sConfigMapName *string, migrationId *string) error {

	if (isNilOrEmpty(accessKey) && !isNilOrEmpty(secretKey)) || (!isNilOrEmpty(accessKey) && isNilOrEmpty(secretKey)) {
		return fmt.Errorf("invalid credentials: please specify both accessKey and secretKey, or neither of them")
	}
	if isNilOrEmpty(accessKey) && isNilOrEmpty(secretKey) && isNilOrEmpty(profileName) {
		return fmt.Errorf("missing credentials: please specify accessKey and secretKey, or the name of the profile to use")
	}
	if !isNilOrEmpty(accessKey) && !isNilOrEmpty(secretKey) && !isNilOrEmpty(profileName) {
		// TODO turn this into a warning in the logs
		fmt.Printf("The profile name will be ignored, as accessKey and secretKey were specified and take precedence\n")
		return nil
	}

	if isNilOrEmpty(region) {
		return fmt.Errorf("missing mandatory region parameter, please specify it")
	}

	if isNilOrEmpty(bucketName) {
		return fmt.Errorf("missing mandatory bucketName parameter, please specify it")
	}

	if isNilOrEmpty(k8sConfigMapName) {
		// TODO turn this into a warning in the logs
		fmt.Printf("No K8s config map name was specified: the migration global state will not be persisted to any config map \n")
		return nil
	}

	if isNilOrEmpty(migrationId) {
		return fmt.Errorf("missing mandatory migrationId parameter, please specify it")
	}

	return nil
}

func isNilOrEmpty(str *string) bool {
	return str == nil || *str == ""
}

// TODO check if necessary, perhaps to format some log messages
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
