package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"slices"
	"strings"
)

type MigrationGlobalState struct {
	KeyspaceCount int   `json:"keyspaceCount"`
	CQLTableCount int   `json:"cqlTableCount"`
	SSTableCount  int   `json:"ssTableCount"`
	DataSize      int64 `json:"dataSize"`
}

type EntityNames struct {
	KeyspaceName      string `json:"keyspaceName"`
	CqlTableName      string `json:"cqlTableName"`
	SsTableNamePrefix string `json:"ssTableNamePrefix"`
}

type SSTable struct {
	EntityNames       *EntityNames `json:"entityNames"`
	Size              int64        `json:"size"`
	KeyPath           string       `json:"keyPath"`
	ComponentFileKeys []string     `json:"componentFileKeys"`
}

type MigrationBucketAccessor struct {
	bucketName           string
	s3Client             *s3.Client
	s3uploader           *s3manager.Uploader
	migrationGlobalState MigrationGlobalState
	migrationId          string
}

const PageSize = 200

func NewMigrationBucketAccessor(accessKey string, secretKey string, profileName string, region string, bucketName string, migrationId string) (*MigrationBucketAccessor, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("missing mandatory bucket name parameter")
	}

	s3Client, err := createS3Client(accessKey, secretKey, profileName, region)
	if err != nil {
		return nil, err
	}

	migrationBucketAccessor := &MigrationBucketAccessor{
		bucketName:           bucketName,
		s3Client:             s3Client,
		s3uploader:           s3manager.NewUploader(s3Client),
		migrationGlobalState: MigrationGlobalState{},
		migrationId:          migrationId,
	}
	return migrationBucketAccessor, nil
}

func NewSSTable(entityNames *EntityNames, keyPrefix string) *SSTable {
	return &SSTable{
		EntityNames:       entityNames,
		KeyPath:           keyPrefix,
		Size:              0,
		ComponentFileKeys: []string{},
	}
}

func (sst *SSTable) addComponentFileKey(key string) {
	sst.ComponentFileKeys = append(sst.ComponentFileKeys, key)
}

func (sst *SSTable) increaseSize(deltaSize int64) {
	sst.Size = sst.Size + deltaSize
}

func createS3Client(accessKey string, secretKey string, profileName string, region string) (*s3.Client, error) {
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
			Region:      region,
			Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
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

func (mba *MigrationBucketAccessor) InitSSTableDescriptors() error {

	listObjectsInputParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(mba.bucketName),
		//MaxKeys: int32(maxTotalKeys),
	}

	// Get the bucket contents with pagination
	maxPageSize := int32(PageSize) // TODO currently a constant - make it configurable?
	p := s3.NewListObjectsV2Paginator(mba.s3Client, listObjectsInputParams, func(paginationOpts *s3.ListObjectsV2PaginatorOptions) {
		if v := maxPageSize; v != 0 {
			paginationOpts.Limit = v
		}
	})

	i := 0

	var currentSSTable *SSTable
	for p.HasMorePages() {
		i++
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return fmt.Errorf("failed to get page %v, %v", i, err)
		}
		fmt.Printf("Reading page %v, containing %v keys\n", i, page.KeyCount)
		for _, object := range page.Contents {
			objectKey := *object.Key
			objectSize := object.Size
			if isSSTableComponentFile(objectKey) {
				entityNames := extractEntityNamesFromKey(objectKey)

				if currentSSTable == nil {
					currentSSTable = NewSSTable(entityNames, extractKeyPath(objectKey))
					mba.updateGlobalBucketState(true,
						true,
						true,
						0)
				} else {
					if isDifferentSSTable(currentSSTable, extractKeyPath(objectKey), entityNames.SsTableNamePrefix) {
						err := mba.persistSSTableDescriptor(currentSSTable)
						if err != nil {
							return err
						}
						mba.updateGlobalBucketState(currentSSTable.EntityNames.KeyspaceName != entityNames.KeyspaceName,
							currentSSTable.EntityNames.CqlTableName != entityNames.CqlTableName,
							currentSSTable.EntityNames.SsTableNamePrefix != entityNames.SsTableNamePrefix,
							currentSSTable.Size)
						currentSSTable = NewSSTable(entityNames, extractKeyPath(objectKey))
					}
				}
				currentSSTable.addComponentFileKey(objectKey)
				currentSSTable.increaseSize(objectSize)

			} else {
				// TODO log at some low level: fmt.Printf("[[not an SSTable component: %v]]\n", objectKey)
			}
		}
	}

	// complete and persist the last SSTable
	err := mba.persistSSTableDescriptor(currentSSTable)
	if err != nil {
		return err
	}

	return mba.persistMigrationGlobalStateDescriptorToBucket()

}

func isSSTableComponentFile(key string) bool {
	// go by extension for now, it may be refined in the future. the naming pattern is too variable
	componentFileExtensions := []string{"CompressionInfo.db", "Data.db", "Digest.crc32", "Filter.db", "Partitions.db",
		"Rows.db", "Statistics.db", "TOC.txt"}
	fileExtensionStartIndex := strings.LastIndex(key, "-")
	return slices.Contains(componentFileExtensions, key[fileExtensionStartIndex+1:])
}

func extractEntityNamesFromKey(key string) *EntityNames {
	keyComponents := strings.Split(key, "/")

	keyspaceNameComponent := keyComponents[len(keyComponents)-5]
	cqlTableNameComponent := keyComponents[len(keyComponents)-4]
	ssTableNamePrefixComponent := keyComponents[len(keyComponents)-1]
	return &EntityNames{
		KeyspaceName:      keyspaceNameComponent,
		CqlTableName:      cqlTableNameComponent[:strings.LastIndex(cqlTableNameComponent, "-")],
		SsTableNamePrefix: ssTableNamePrefixComponent[:strings.LastIndex(ssTableNamePrefixComponent, "-")],
	}
}

/*
Returns the "path" from the key - everything up to the last slash excluded.
*/
func extractKeyPath(key string) string {
	return key[:strings.LastIndex(key, "/")]
}

func isDifferentSSTable(currentSSTable *SSTable, keyPath string, ssTableNamePrefix string) bool {
	return (currentSSTable.KeyPath != keyPath) || (currentSSTable.EntityNames.SsTableNamePrefix != ssTableNamePrefix)
}

func (mba *MigrationBucketAccessor) persistSSTableDescriptor(ssTable *SSTable) error {
	// Create JSON content from struct
	marshalledContent, err := json.Marshal(ssTable)
	if err != nil {
		return fmt.Errorf("error generating the JSON descriptor for SSTable %v/%v: %v", ssTable.KeyPath, ssTable.EntityNames.SsTableNamePrefix, err)
	}
	ssTableDescriptorJson := string(marshalledContent)
	fmt.Printf("JSON string created:\n%s\n", ssTableDescriptorJson)

	// Create globally unique identifier for SSTable, appending a UUID to the name prefix
	ssTableUniqueIdentifier := strings.Join([]string{ssTable.EntityNames.SsTableNamePrefix, uuid.New().String()}, "-")

	// Create s3 object key
	descriptorKey := strings.Join([]string{mba.migrationId,
		"SSTableDescriptors",
		ssTable.EntityNames.KeyspaceName,
		ssTable.EntityNames.CqlTableName,
		ssTableUniqueIdentifier},
		"/")

	return mba.persistDescriptorToBucket(descriptorKey, ssTableDescriptorJson)
}

func (mba *MigrationBucketAccessor) persistMigrationGlobalStateDescriptorToBucket() error {
	marshalledContent, err := json.Marshal(mba.migrationGlobalState)
	if err != nil {
		return fmt.Errorf("error generating the JSON descriptor for the migration global state: %v", err)
	}
	migrationGlobalStateDescriptorJson := string(marshalledContent)
	descriptorKey := strings.Join([]string{mba.migrationId, "globalState"}, "/")
	return mba.persistDescriptorToBucket(descriptorKey, migrationGlobalStateDescriptorJson)
}

func (mba *MigrationBucketAccessor) persistDescriptorToBucket(descriptorKey string, descriptorContent string) error {

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(mba.bucketName),
		Key:    aws.String(descriptorKey),
		Body:   strings.NewReader(descriptorContent),
	}

	uploadOutput, err := mba.s3uploader.Upload(context.Background(), uploadInput)

	if err != nil {
		fmt.Errorf("couldn't upload file to %v with key %v due to: %v\n", mba.bucketName, descriptorKey, err)
	}

	fmt.Printf("Uploaded file to location: %v\n", uploadOutput.Location)
	return nil
}

func (mba *MigrationBucketAccessor) updateGlobalBucketState(incrKeyspaceCount bool, incrCQLTableCount bool, incrSSTableCount bool, dataSizeDelta int64) {
	if incrKeyspaceCount {
		mba.migrationGlobalState.KeyspaceCount++
	}
	if incrCQLTableCount {
		mba.migrationGlobalState.CQLTableCount++
	}
	if incrSSTableCount {
		mba.migrationGlobalState.SSTableCount++
	}
	mba.migrationGlobalState.DataSize = mba.migrationGlobalState.DataSize + dataSizeDelta
}
