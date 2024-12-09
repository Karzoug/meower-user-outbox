package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/Karzoug/meower-common-go/postgresql"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/Karzoug/meower-user-outbox/internal/entity"
)

type eventConsumer struct {
	db          postgresql.DB
	producer    producer
	cfg         Config
	errorsCount int
	logger      zerolog.Logger
}

func NewEventConsumer(cfg Config, db postgresql.DB, producer producer, logger zerolog.Logger) eventConsumer {
	logger = logger.With().
		Str("component", "event consumer").
		Logger()
	return eventConsumer{
		db:       db,
		producer: producer,
		cfg:      cfg,
		logger:   logger,
	}
}

func (ec eventConsumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		count, err := ec.handleBatchEvents(ctx)
		if err != nil {
			ec.errorsCount++
			ec.logger.Error().
				Err(err).
				Msg("error handling batch events")
			if ec.errorsCount >= ec.cfg.MaxErrorsCount {
				return fmt.Errorf("%d attempts failed, exit, last error: %w", count, err)
			}
			time.Sleep(ec.cfg.CheckInterval)
			continue
		}
		ec.errorsCount = 0

		if count == 0 {
			time.Sleep(ec.cfg.CheckInterval)
			continue
		}

		ec.logger.Info().
			Int("count", count).
			Msg("user table changed events handled")
	}
}

func (ec eventConsumer) handleBatchEvents(ctx context.Context) (int, error) {
	const op = "postgresql: handle batch events"
	ctx, cancel := context.WithTimeout(ctx, ec.cfg.HandleBatchTimeout)
	defer cancel()

	events, err := ec.listUnhandledEvents(ctx)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	if err := ec.producer.Produce(ctx, events); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	ids := make([]int64, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}

	if err := ec.deleteEvents(ctx, ids); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return len(ids), nil
}

func (ec eventConsumer) deleteEvents(ctx context.Context, ids []int64) error {
	const (
		op    = "postgresql: delete events"
		query = "DELETE FROM outbox WHERE id = ANY(@ids)"
	)

	_, err := ec.db.Exec(ctx, query, pgx.NamedArgs{
		"ids": ids,
	})
	if err != nil {
		return fmt.Errorf("%s: query error: %w", op, err)
	}

	return nil
}

func (ec eventConsumer) listUnhandledEvents(ctx context.Context) ([]entity.UserChangedEvent, error) {
	const (
		op    = "postgresql: list unhandled events"
		query = `
WITH rows AS (
  select id
  from outbox
  where (reserved_to IS null) OR (reserved_to > CURRENT_TIMESTAMP)
  ORDER BY id
  LIMIT 10
)
UPDATE outbox
SET reserved_to = '2024-12-06 22:33:15.071 +0300'
WHERE EXISTS (SELECT * FROM rows WHERE outbox.id = rows.id)
returning id, change_type, user_id, created_at`
	)

	rows, err := ec.db.Query(ctx, query, pgx.NamedArgs{
		"reserved_at": time.Now().Add(ec.cfg.ReservedTimeout),
		"limit":       ec.cfg.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: query error: %w", op, err)
	}
	defer rows.Close()

	events, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[entity.UserChangedEvent])
	if err != nil {
		return nil, fmt.Errorf("%s: convert to event type error: %w", op, err)
	}

	return events, nil
}
