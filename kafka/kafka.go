package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/caarlos0/env"
	"os"
	"time"
)

type Config struct {
	Topic         string   `env:"TOPIC"`
	Brokers       []string `env:"BROKER" envSeparator:","`
	ConsumerGroup string   `env:"CONSUMER_GROUP"`
}

func newConsumer(conf *Config) (*cluster.Consumer, error) {
	consumerCfg := cluster.NewConfig()
	consumerCfg.Consumer.Return.Errors = true
	consumerCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerCfg.Group.Return.Notifications = true
	consumerCfg.Consumer.Offsets.CommitInterval = time.Second

	consumer, err := cluster.NewConsumer(conf.Brokers, conf.ConsumerGroup, []string{conf.Topic}, consumerCfg)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func Init() (*cluster.Consumer, error) {
	conf := Config{}
	if err := env.Parse(&conf); err != nil {
		return nil, errors.New("cloud not load kafka environment variables")
	}

	return newConsumer(&conf)
}

func Topic() string {
	return os.Getenv("TOPIC")
}
