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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"slices"
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
	migrationGlobalState *MigrationGlobalState
	k8sConfigMapName     string
}

const PageSize = 200

func NewMigrationBucketAccessor(accessKey string, secretKey string, profileName string, region string,
	bucketName string, k8sConfigMapName string, migrationId string) (*MigrationBucketAccessor, error) {

	s3Client, err := createS3Client(accessKey, secretKey, profileName, region)
	if err != nil {
		return nil, err
	}

	migrationBucketAccessor := &MigrationBucketAccessor{
		bucketName:           bucketName,
		s3Client:             s3Client,
		s3uploader:           s3manager.NewUploader(s3Client),
		migrationGlobalState: NewMigrationGlobalState(migrationId),
		k8sConfigMapName:     k8sConfigMapName,
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

func (mba *MigrationBucketAccessor) InitAndPersistMigrationDescriptors() error {

	listObjectsInputParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(mba.bucketName),
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
					if isDifferentSSTable(currentSSTable, extractKeyPath(objectKey), entityNames.SsTableNamePrefix) {
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

	return persistObjectToBucket(mba.s3uploader, mba.bucketName, descriptorKey, ssTableDescriptorJson)
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
	err = persistObjectToBucket(mba.s3uploader, mba.bucketName, descriptorKey, migrationGlobalStateDescriptorJson)
	if err != nil {
		return err
	}

	// persist the relevant migration information to the k8s config map
	stateMap := make(map[string]string)
	stateMap[strings.Join([]string{"sstable-descriptor-key-prefix", mba.migrationGlobalState.MigrationId}, "-")] = mba.migrationGlobalState.SSTableDescriptorKeyPrefix
	stateMap[strings.Join([]string{"total-sstable-count", mba.migrationGlobalState.MigrationId}, "-")] = strconv.Itoa(mba.migrationGlobalState.SSTableCount)
	if mba.k8sConfigMapName != "" {
		err = writeStateToConfigMap(stateMap)
		fmt.Printf("Written state to k8s configMap for migrationId %v\n", mba.migrationGlobalState.MigrationId)
	} else {
		fmt.Printf("Disabled functionality that writes state to k8s configMap for migrationId %v.\nThe state to be written would have been:\n", mba.migrationGlobalState.MigrationId)
		for k, v := range stateMap {
			fmt.Printf("StateMap entry: %v --> %v\n", k, v)
		}
	}

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

// rename to indicate which config map it writes to
func writeStateToConfigMap(stateMap map[string]string) error {

	//TODO populate with the right values
	config := &rest.Config{
		Host:                "",
		APIPath:             "",
		ContentConfig:       rest.ContentConfig{},
		Username:            "",
		Password:            "",
		BearerToken:         "",
		BearerTokenFile:     "",
		Impersonate:         rest.ImpersonationConfig{},
		AuthProvider:        nil,
		AuthConfigPersister: nil,
		ExecProvider:        nil,
		TLSClientConfig:     rest.TLSClientConfig{},
		UserAgent:           "",
		DisableCompression:  false,
		Transport:           nil,
		WrapTransport:       nil,
		QPS:                 0,
		Burst:               0,
		RateLimiter:         nil,
		WarningHandler:      nil,
		Timeout:             0,
		Dial:                nil,
		Proxy:               nil,
	}
	clientSet, err := kubernetes.NewForConfig(config)

	// Retrieve the existing ConfigMap
	// TODO populate with the right values
	namespace := "namespace"
	configMapName := "template-conf"
	configMap, err := clientSet.CoreV1().ConfigMaps(namespace).Get(context.Background(), configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error retrieving the k8s configMap with namespace %v and name %v, due to %v", namespace, configMapName, err)
	}

	// Modify the data in the ConfigMap
	for stateKey, stateValue := range stateMap {
		configMap.Data[stateKey] = stateValue
	}

	// Update the ConfigMap
	updatedConfigMap, err := clientSet.CoreV1().ConfigMaps(namespace).Update(context.Background(), configMap, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating the k8s configMap with namespace %v and name %v, due to %v", namespace, configMapName, err)
	}

	for stateKey, _ := range stateMap {
		fmt.Printf("Updated configMap with: %v --> %v\n", stateKey, updatedConfigMap.Data[stateKey])
	}
	return nil
}

// TODO probably unnecessary - should be ok to remove
//func (mba *MigrationBucketAccessor) GetMigrationGlobalDescriptor() (string, error) {
//
//	descriptorKey := strings.Join([]string{mba.migrationId, "globalState"}, "/")
//	descriptorJsonContent, err := retrieveObjectContentFromBucket(mba.s3Client, mba.bucketName, descriptorKey)
//	if err != nil {
//		return "", err
//	}
//	return descriptorJsonContent, nil
//}
