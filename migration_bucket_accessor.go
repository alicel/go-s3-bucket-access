package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alicel/go-s3-bucket-access/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"strconv"
	"strings"
)

type MigrationGlobalState struct {
	MigrationId                string `json:"migrationId"`
	KeyspaceCount              int    `json:"keyspaceCount"`
	CQLTableCount              int    `json:"cqlTableCount"`
	SSTableCount               int    `json:"ssTableCount"`
	DataSize                   int64  `json:"dataSize"`
	SSTableDescriptorKeyPrefix string `json:"ssTableDescriptorKeyPrefix"`
}

type MigrationBucketAccessor struct {
	accessorConfig       *config.AccessorConfig
	s3Client             *s3.Client
	s3uploader           *s3manager.Uploader
	migrationGlobalState *MigrationGlobalState
}

const PageSize = 200

func NewMigrationBucketAccessor(ac *config.AccessorConfig) (*MigrationBucketAccessor, error) {

	s3Client, err := createS3Client(ac)
	if err != nil {
		return nil, err
	}

	migrationBucketAccessor := &MigrationBucketAccessor{
		accessorConfig:       ac,
		s3Client:             s3Client,
		s3uploader:           s3manager.NewUploader(s3Client),
		migrationGlobalState: NewMigrationGlobalState(ac.MigrationId),
	}
	return migrationBucketAccessor, nil
}

func NewMigrationGlobalState(migrationId string) *MigrationGlobalState {

	return &MigrationGlobalState{
		MigrationId:                migrationId,
		KeyspaceCount:              0,
		CQLTableCount:              0,
		SSTableCount:               0,
		DataSize:                   0,
		SSTableDescriptorKeyPrefix: strings.Join([]string{migrationId, "SSTableDescriptors"}, "/"),
	}
}

func (mba *MigrationBucketAccessor) InitAndPersistMigrationDescriptors() error {

	listObjectsInputParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(mba.accessorConfig.BucketName),
	}

	// Get the bucket contents with pagination
	maxPageSize := int32(PageSize) // TODO currently a constant - make it configurable?
	p := s3.NewListObjectsV2Paginator(mba.s3Client, listObjectsInputParams, func(paginationOpts *s3.ListObjectsV2PaginatorOptions) {
		if v := maxPageSize; v != 0 {
			paginationOpts.Limit = v
		}
	})

	i := 0

	ssTableSequenceNumber := 0
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
					mba.updateMigrationGlobalState(true,
						true,
						true,
						0)
				} else {
					if currentSSTable.isDifferentSSTable(extractKeyPath(objectKey), entityNames.SsTableNamePrefix) {
						ssTableSequenceNumber++
						err := mba.persistSSTableDescriptor(currentSSTable, ssTableSequenceNumber)
						if err != nil {
							return err
						}
						mba.updateMigrationGlobalState(currentSSTable.EntityNames.KeyspaceName != entityNames.KeyspaceName,
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
	err := mba.persistSSTableDescriptor(currentSSTable, ssTableSequenceNumber)
	if err != nil {
		return err
	}

	err = mba.persistMigrationGlobalStateDescriptor()
	if err != nil {
		return err
	}

	return nil
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

func (mba *MigrationBucketAccessor) persistSSTableDescriptor(ssTable *SSTable, ssTableSequenceNumber int) error {
	// Create JSON content from struct
	marshalledContent, err := json.Marshal(ssTable)
	if err != nil {
		return fmt.Errorf("error generating the JSON descriptor for SSTable %v/%v: %v", ssTable.KeyPath, ssTable.EntityNames.SsTableNamePrefix, err)
	}
	ssTableDescriptorJson := string(marshalledContent)
	fmt.Printf("JSON string created:\n%s\n", ssTableDescriptorJson)

	// Create globally unique identifier for SSTable, appending a sequence number to the name prefix
	ssTableUniqueIdentifier := strings.Join([]string{ssTable.EntityNames.SsTableNamePrefix, strconv.Itoa(ssTableSequenceNumber)}, "-")

	// Create s3 object key: <migrationId>/SSTableDescriptors/<seqNum>/descriptor-<seqNum>
	descriptorKey := strings.Join([]string{mba.migrationGlobalState.SSTableDescriptorKeyPrefix,
		strconv.Itoa(ssTableSequenceNumber),
		ssTableUniqueIdentifier}, "/")

	return persistObjectToBucket(mba.s3uploader, mba.accessorConfig.BucketName, descriptorKey, ssTableDescriptorJson)
}

func (mba *MigrationBucketAccessor) persistMigrationGlobalStateDescriptor() error {
	// persist the whole migration descriptor to S3
	marshalledContent, err := json.Marshal(mba.migrationGlobalState)
	if err != nil {
		return fmt.Errorf("error generating the JSON descriptor for the migration global state: %v", err)
	}
	migrationGlobalStateDescriptorJson := string(marshalledContent)
	descriptorName := strings.Join([]string{"globalState", mba.migrationGlobalState.MigrationId}, "-")
	descriptorKey := strings.Join([]string{mba.migrationGlobalState.MigrationId, descriptorName}, "/")
	err = persistObjectToBucket(mba.s3uploader, mba.accessorConfig.BucketName, descriptorKey, migrationGlobalStateDescriptorJson)
	if err != nil {
		return err
	}

	// persist the relevant migration information to the k8s config map
	err = mba.writeStateToConfigMap()

	return err
}

func (mba *MigrationBucketAccessor) updateMigrationGlobalState(incrKeyspaceCount bool, incrCQLTableCount bool, incrSSTableCount bool, dataSizeDelta int64) {
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

func (mba *MigrationBucketAccessor) writeStateToConfigMap() error {

	stateMap := make(map[string]string)
	stateMap[strings.Join([]string{"sstable-descriptor-key-prefix", mba.migrationGlobalState.MigrationId}, "-")] = mba.migrationGlobalState.SSTableDescriptorKeyPrefix
	stateMap[strings.Join([]string{"total-sstable-count", mba.migrationGlobalState.MigrationId}, "-")] = strconv.Itoa(mba.migrationGlobalState.SSTableCount)

	if mba.accessorConfig.K8sConfigMapNamespace == "" || mba.accessorConfig.K8sConfigMapName == "" {
		fmt.Printf("Disabled functionality that writes state to k8s configMap for migrationId %v.\nThe state to be written would have been:\n", mba.migrationGlobalState.MigrationId)
		for k, v := range stateMap {
			fmt.Printf("StateMap entry: %v --> %v\n", k, v)
		}
		return nil
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	clientSet, err := kubernetes.NewForConfig(config)

	// Retrieve the existing ConfigMap
	configMap, err := clientSet.CoreV1().ConfigMaps(mba.accessorConfig.K8sConfigMapNamespace).Get(context.Background(), mba.accessorConfig.K8sConfigMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error retrieving the k8s configMap with namespace %v and name %v, due to %v", mba.accessorConfig.K8sConfigMapNamespace, mba.accessorConfig.K8sConfigMapName, err)
	}

	// Modify the data in the ConfigMap
	for stateKey, stateValue := range stateMap {
		configMap.Data[stateKey] = stateValue
	}

	// Update the ConfigMap
	updatedConfigMap, err := clientSet.CoreV1().ConfigMaps(mba.accessorConfig.K8sConfigMapNamespace).Update(context.Background(), configMap, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating the k8s configMap with namespace %v and name %v, due to %v", mba.accessorConfig.K8sConfigMapNamespace, mba.accessorConfig.K8sConfigMapName, err)
	}

	for stateKey, _ := range stateMap {
		fmt.Printf("Updated configMap with: %v --> %v\n", stateKey, updatedConfigMap.Data[stateKey])
	}

	fmt.Printf("Written state to k8s configMap for migrationId %v\n", mba.migrationGlobalState.MigrationId)

	return nil
}
