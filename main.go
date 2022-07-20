package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config, err := NewConfig()
	if err != nil {
		panic(err)
	}
	configMap, err := config.KafkaConfigMap()
	if err != nil {
		panic(err)
	}
	adminClient, err := kafka.NewAdminClient(configMap)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	resources := []kafka.ConfigResource{
		{
			Type: kafka.ResourceTopic,
			Name: "se_decomposition.lmpd_raw_to_logical.pjm.20220315T1516Z",
		},
	}
	results, err := adminClient.DescribeConfigs(ctx, resources)
	if err != nil {
		panic(err)
	}
	if len(results) != 1 {
		panic(fmt.Sprintf("expected 1 result, got %d", len(results)))
	}
	var (
		maxKeyLength = 1
		result       = results[0]
	)
	for configName, _ := range result.Config {
		if len(configName) > maxKeyLength {
			maxKeyLength = len(configName)
		}
	}
	formatString := fmt.Sprintf("%%-%ds = %%s\n", maxKeyLength)

	for configName, entry := range result.Config {
		fmt.Printf(formatString, configName, entry.Value)
	}
}

type Config struct {
	AuthDir     string
	ClusterName string
	TopicName   string
}

func NewConfig() (Config, error) {
	aivenAuthDir := os.Getenv("AIVEN_AUTH_DIR")

	var config Config
	flag.StringVar(&config.AuthDir, "authdir", aivenAuthDir, "Directory containing files for SSL auth.")
	flag.StringVar(&config.ClusterName, "c", "aiven", "Cluster name (aiven, confluent).")
	flag.StringVar(&config.TopicName, "t", "", "Topic name (required).")
	flag.Parse()

	return config, nil
}

func (config Config) KafkaConfigMap() (*kafka.ConfigMap, error) {
	switch config.ClusterName {
	case "aiven":
		return &kafka.ConfigMap{
			"bootstrap.servers":        "kafka-2851c2ea-ross-30b9.aivencloud.com:24942",
			"security.protocol":        "SSL",
			"ssl.ca.location":          filepath.Join(config.AuthDir, "cafile"),
			"ssl.certificate.location": filepath.Join(config.AuthDir, "certfile"),
			"ssl.key.location":         filepath.Join(config.AuthDir, "keyfile"),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported cluster name: %s\n", config.ClusterName)
	}
}
