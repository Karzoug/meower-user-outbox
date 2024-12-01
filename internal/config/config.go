package config

import "log/slog"

type Config struct {
	LogLevel slog.Level   `env:"LOG_LEVEL" envDefault:"info"`
	PG       pgConfig     `envPrefix:"PG_"`
	Kafka    kafkaConfig  `envPrefix:"KAFKA_"`
	Metric   metricConfig `envPrefix:"METRIC_"`
}

type pgConfig struct {
	URI string `env:"URI,required"`
}

type kafkaConfig struct {
	Brokers []string `env:"BROKERS,notEmpty" envSeparator:","`
}

type metricConfig struct {
	Port int `env:"PORT,required"`
}
