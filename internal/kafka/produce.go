package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	ck "github.com/Karzoug/meower-common-go/kafka"
	"github.com/Karzoug/meower-user-outbox/internal/entity"
	userApi "github.com/Karzoug/meower-user-outbox/internal/kafka/gen/user/v1"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

// Produce sends user changed events to kafka broker.
// It renurns nil error only if all events were sent successfully.
func (ep *eventProducer) Produce(ctx context.Context, events []entity.UserChangedEvent) error {
	var (
		val   []byte
		resCh chan kafka.Event
		err   error
	)

	fngpnt := ck.MessageTypeHeaderValue(&userApi.ChangedEvent{})

	for i, event := range events {
		var ct userApi.ChangeType
		switch event.ChangeType {
		case entity.ChangeTypeCreate:
			ct = userApi.ChangeType_CHANGE_TYPE_CREATED
		case entity.ChangeTypeDelete:
			ct = userApi.ChangeType_CHANGE_TYPE_DELETED
		}

		e := &userApi.ChangedEvent{
			Id:         event.UserID.String(),
			ChangeType: ct,
		}

		val, err = proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		if i == len(events)-1 {
			resCh = make(chan kafka.Event, 1)
		}

		for range 5 { // <-- only if producer queue is full
			err = ep.p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
				Headers: []kafka.Header{
					{
						Key:   ck.MessageTypeHeaderKey,
						Value: []byte(fngpnt),
					},
				},
				Opaque: event.ID,
				Value:  val,
			}, resCh)
			if nil == err {
				ep.rec.IncKafkaClientProduceCounter(ctx, topicName)
				return nil
			}

			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) {
				if kafkaErr.Code() == kafka.ErrQueueFull {
					time.Sleep(500 * time.Millisecond) // <-- only if producer queue is full
					continue
				}
			}

			return fmt.Errorf("failed to produce message: %w", err)
		}

	}

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	select {
	case res := <-resCh:
		// log and collect metrics here
		_ = ep.handleKafkaEvent(res)

		if m, ok := res.(*kafka.Message); ok {
			if nil == m.TopicPartition.Error {
				// only this path is success
				return nil
			}
		}
		return fmt.Errorf("failed to produce message")
	case <-ctx.Done():
		return fmt.Errorf("failed to produce message: %w", ctx.Err())
	}
}
