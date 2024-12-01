package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Karzoug/meower-user-outbox/internal/config"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdscfg "github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

const (
	schema               = "public"
	tableName            = "users"
	topicName            = "users"
	publicationName      = "cds_publication"
	publicationTableName = "users"
	slotName             = "cdc_slot"
)

type connector struct {
	c cdc.Connector
}

func NewConnector(ctx context.Context, cfg config.Config, logger *slog.Logger) (connector, func(ctx context.Context) error, error) {
	pgCfg, err := pgx.ParseConfig(cfg.PG.URI)
	if err != nil {
		return connector{}, nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	ccfg := cdscfg.Connector{
		CDC: cdcconfig.Config{
			Host:      pgCfg.Host,
			Username:  pgCfg.User,
			Password:  pgCfg.Password,
			Database:  pgCfg.Database,
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              publicationName,
				Operations: publication.Operations{
					publication.OperationInsert,
				},
				Tables: publication.Tables{
					publication.Table{
						Name:            publicationTableName,
						ReplicaIdentity: publication.ReplicaIdentityFull,
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        slotName,
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: cfg.Metric.Port,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: cfg.LogLevel,
			},
		},
		Kafka: cdscfg.Kafka{
			TableTopicMapping: map[string]string{
				schema + tableName: topicName,
			},
			Brokers:                     cfg.Kafka.Brokers,
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 200,
		},
	}

	c, err := cdc.NewConnector(ctx, ccfg, handler(logger))
	if err != nil {
		return connector{}, nil, fmt.Errorf("create new connector failed: %w", err)
	}

	return connector{c: c},
		func(ctx context.Context) error {
			c.Close()
			return nil
		}, nil
}

func (c connector) Run(ctx context.Context) error {
	c.c.Start(ctx)

	return nil
}
