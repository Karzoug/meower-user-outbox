package config

import (
	"github.com/Karzoug/meower-common-go/metric/prom"
	"github.com/Karzoug/meower-common-go/postgresql"
	"github.com/rs/zerolog"

	"github.com/Karzoug/meower-user-outbox/internal/kafka"
	"github.com/Karzoug/meower-user-outbox/internal/pg"
)

type Config struct {
	LogLevel zerolog.Level     `env:"LOG_LEVEL" envDefault:"info"`
	PG       postgresql.Config `envPrefix:"PG_"`
	Kafka    kafka.Config      `envPrefix:"KAFKA_"`
	PromHTTP prom.ServerConfig `envPrefix:"PROM_"`
	Consumer pg.Config         `envPrefix:"CONSUMER_"`
	Producer pg.Config         `envPrefix:"PRODUCER_"`
}
