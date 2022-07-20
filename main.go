package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"

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
			Name: config.TopicName,
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
		entries      = ConfigEntryResults{}
		maxKeyLength = 1
		result       = results[0]
	)
	for _, entry := range result.Config {
		if len(entry.Name) > maxKeyLength {
			maxKeyLength = len(entry.Name)
		}
		entries = append(entries, entry)
	}
	formatString := fmt.Sprintf("%%-%ds = %%s\n", maxKeyLength)

	sort.Sort(&entries)

	for _, entry := range entries {
		fmt.Printf(formatString, entry.Name, entry.Value)
	}
}

type Config struct {
	ConfigStrings FlagValues
	TopicName     string
}

func NewConfig() (Config, error) {
	var config Config
	flag.Var(&config.ConfigStrings, "X", "Configuration.")
	flag.StringVar(&config.TopicName, "t", "", "Topic name (required).")
	flag.Parse()

	return config, nil
}

func (config Config) KafkaConfigMap() (*kafka.ConfigMap, error) {
	cm := kafka.ConfigMap{}
	for _, cs := range config.ConfigStrings.Values {
		parts := strings.Split(cs, "=")
		if len(parts) < 2 {
			return nil, errors.New("Format expected for kafka configs: -X KEY=VALUE")
		}
		cm[parts[0]] = strings.Join(parts[1:], "=")
	}
	return &cm, nil
}

// ConfigEntryResults is a list of kafka config entry results that can be sorted
// Go's stdlib sort package.
type ConfigEntryResults []kafka.ConfigEntryResult

// Len returns the length of the list.
func (e *ConfigEntryResults) Len() int {
	if e == nil {
		return 0
	}
	return len(*e)
}

// Less returns true if entry i < entry j. False otherwise.
func (e *ConfigEntryResults) Less(i, j int) bool {
	return strings.Compare((*e)[i].Name, (*e)[j].Name) < 0
}

// Swap swaps two entries in the list.
func (e *ConfigEntryResults) Swap(i, j int) {
	tmp := (*e)[i]
	(*e)[i] = (*e)[j]
	(*e)[j] = tmp
}

// FlagValues makes it possible to set a flag multiple times.
type FlagValues struct {
	Values []string
}

// String converts the flag values into a string by joining the values with '|'
func (fv *FlagValues) String() string {
	if fv == nil {
		return ""
	}
	return strings.Join(fv.Values, "|")
}

func (fv *FlagValues) Set(s string) error {
	fv.Values = append(fv.Values, s)
	return nil
}
