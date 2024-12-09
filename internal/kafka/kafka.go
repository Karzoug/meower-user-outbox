package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

const defaultCloseTimeout = 10 * time.Second

var topicName = "users"

type eventProducer struct {
	p      *kafka.Producer
	rec    kafkaRecorder
	cfg    Config
	logger zerolog.Logger
}

func NewProducer(ctx context.Context, cfg Config, rec kafkaRecorder, logger zerolog.Logger) (*eventProducer, func(context.Context) error, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        cfg.Brokers,
		"enable.idempotence":       cfg.EnableIdempotence,
		"allow.auto.create.topics": true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create producer: %w", err)
	}

	logger = logger.With().
		Str("component", "kafka producer").
		Logger()

	var timeoutMs int
	if t, ok := ctx.Deadline(); ok {
		timeoutMs = int(time.Until(t).Milliseconds())
	} else {
		timeoutMs = 500
	}

	// analog PING here
	_, err = p.GetMetadata(&topicName, false, timeoutMs)
	if err != nil {
		return nil, nil, fmt.Errorf("create kafka producer: failed to get metadata: %w", err)
	}

	ep := &eventProducer{
		p:      p,
		cfg:    cfg,
		rec:    rec,
		logger: logger,
	}

	return ep, ep.Close, nil
}

// Run starts loop kafka events handling for logs and metrics.
func (ep *eventProducer) Run(ctx context.Context) error {
	for {
		select {
		case event := <-ep.p.Events():
			if err := ep.handleKafkaEvent(event); err != nil {
				return err
			}
			continue
		default:
		}

		select {
		case event := <-ep.p.Events():
			if err := ep.handleKafkaEvent(event); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (ep *eventProducer) Close(ctx context.Context) error {
	ep.logger.Info().
		Msg("flushing outstanding messages")

	timeout := defaultCloseTimeout
	t, ok := ctx.Deadline()
	if ok {
		timeout = time.Until(t)
	}

	ep.p.Flush(int(timeout.Milliseconds()))

	ep.p.Close()

	return nil
}

func (ep *eventProducer) handleKafkaEvent(e kafka.Event) error {
	switch ev := e.(type) {
	case *kafka.Message:
		m := ev
		if m.TopicPartition.Error != nil {
			ep.rec.IncKafkaProducerErrorCounter(context.TODO(), *m.TopicPartition.Topic)
			ep.logger.Error().
				Err(m.TopicPartition.Error).
				Msg("message delivery failed")
		} else {
			ep.rec.IncKafkaProducerSuccessCounter(context.TODO(), *m.TopicPartition.Topic)
			ep.logger.Info().
				Str("topic", *m.TopicPartition.Topic).
				Int("partition", int(m.TopicPartition.Partition)).
				Int64("offset", int64(m.TopicPartition.Offset)).
				Int64("eventID", m.Opaque.(int64)).
				Msg("message delivered")
		}

	case kafka.Error:
		// Generic client instance-level errors, such as
		// broker connection failures, authentication issues, etc.
		//
		// These errors should generally be considered informational
		// as the underlying client will automatically try to
		// recover from any errors encountered, the application
		// does not need to take action on them.
		//
		// But with idempotence enabled, truly fatal errors can
		// be raised when the idempotence guarantees can't be
		// satisfied, these errors are identified by
		// `e.IsFatal()`.

		e := ev
		if e.IsFatal() {
			// Fatal error handling.
			//
			// When a fatal error is detected by the producer
			// instance, it will emit kafka.Error event (with
			// IsFatal()) set on the Events channel.
			//
			// Note:
			//   After a fatal error has been raised, any
			//   subsequent Produce*() calls will fail with
			//   the original error code.
			return fmt.Errorf("fatal error is detected by the producer instance: %w", e)
		}
		ep.logger.Error().
			Err(e).
			Msg("producer instance error")
	}

	return nil
}
