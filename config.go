package main

import (
	"encoding/json"
	"fmt"
	"github.com/kelseyhightower/envconfig"
)

/* Environment variables:

Credentials:
 - MBA_ACCESS_KEY: A valid access key for your AWS account. Requires a Secret Key as well.
 - MBA_SECRET_KEY: The secret key for the access key that you specified. Requires an Access Key as well.
 - MBA_PROFILE_NAME: The AWS profile name to use.
 - MBA_REGION: The AWS region where your S3 bucket is. Always required.
 - MBA_BUCKET_NAME: The name of your S3 bucket. Always required.
 - MBA_MIGRATION_ID: The identifier of this migration. Always required.
 - MBA_K8S_CONFIG_MAP_NAME: The name of the config map to write to (Also requires a namespace. Leave empty to disable writing to any config map)
 - MBA_K8S_CONFIG_MAP_NAMESPACE: The config map's namespace (Required if a config map name is provided. Leave empty to disable writing to any config map)

Notes:
 - Either [Access key + Secret Key] or a profile name are required. If all are specified, Access Key + Secret Key take precedence over the profile.
 - K8s config map and its namespace must be specified to enable the MBA to write to a K8s config map.
*/

type AccessorConfig struct {
	AccessKey             string `default:"" split_words:"true"`
	SecretKey             string `default:"" split_words:"true"`
	ProfileName           string `default:"" split_words:"true"`
	Region                string `required:"true" split_words:"true"`
	BucketName            string `required:"true" split_words:"true"`
	MigrationId           string `required:"true" split_words:"true"`
	K8sConfigMapName      string `default:"" split_words:"true"`
	K8sConfigMapNamespace string `default:"" split_words:"true"`
}

func (ac *AccessorConfig) String() string {
	serializedConfig, _ := json.Marshal(ac)
	return string(serializedConfig)
}

func NewAccessorConfig() *AccessorConfig {
	return &AccessorConfig{}
}

// ParseAndValidate fills out the fields of the AccessorConfig struct according to envconfig rules
// See: Usage @ https://github.com/kelseyhightower/envconfig
func (ac *AccessorConfig) ParseAndValidate() (*AccessorConfig, error) {

	err := envconfig.Process("MBA", ac)
	if err != nil {
		return nil, fmt.Errorf("could not load environment variables: %w", err)
	}

	err = ac.validate()
	if err != nil {
		return nil, err
	}
	// TODO change to use a logging library
	fmt.Printf("Parsed configuration: %v\n", ac.String())

	return ac, nil
}

func (ac *AccessorConfig) validate() error {

	if (ac.AccessKey == "" && ac.SecretKey != "") || (ac.AccessKey != "" && ac.SecretKey == "") {
		return fmt.Errorf("invalid credentials: please specify both access key and secret key, or neither of them")
	}

	if ac.AccessKey == "" && ac.SecretKey == "" && ac.ProfileName == "" {
		return fmt.Errorf("missing credentials: please specify both access key and secret key, or the name of the profile to use")
	}
	if ac.AccessKey != "" && ac.SecretKey != "" && ac.ProfileName != "" {
		// TODO turn this into a warning in the logs
		fmt.Printf("The profile name will be ignored, as access key and secret key were specified and take precedence\n")
		return nil
	}

	if ac.Region == "" {
		return fmt.Errorf("missing mandatory region parameter, please specify it")
	}

	if ac.BucketName == "" {
		return fmt.Errorf("missing mandatory bucketName parameter, please specify it")
	}

	if ac.K8sConfigMapName == "" {
		// TODO turn this into a warning in the logs
		fmt.Printf("No K8s config map name was specified: the migration global state will not be persisted to any config map \n")
		return nil
	}

	if ac.MigrationId == "" {
		return fmt.Errorf("missing mandatory migrationId parameter, please specify it")
	}

	return nil
}
