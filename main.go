package main

import (
	"fmt"
	"os"
)

func main() {
	conf, err := NewAccessorConfig().ParseAndValidate()
	migrationBucketAccessor, err := NewMigrationBucketAccessor(conf)
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
