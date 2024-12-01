package outbox

import (
	"log/slog"

	ck "github.com/Karzoug/meower-common-go/kafka"
	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"google.golang.org/protobuf/proto"

	gen "github.com/Karzoug/meower-user-outbox/internal/client/kafka/gen/user/v1"
)

func handler(logger *slog.Logger) func(msg *cdc.Message) []gokafka.Message {
	return func(msg *cdc.Message) []gokafka.Message {
		if msg.TableName == tableName {
			if msg.Type.IsInsert() {
				return userCreatedHandler(msg, logger)
			}
		}

		return []gokafka.Message{}
	}
}

func userCreatedHandler(msg *cdc.Message, logger *slog.Logger) []gokafka.Message {
	l := logger.With(
		slog.String("table", msg.TableName),
		slog.String("table operation", "insert"),
	)

	id, ok := msg.NewData["id"].(string)
	if !ok {
		l.Error("invalid id format",
			slog.Any("id", msg.NewData["id"]),
		)
		return []gokafka.Message{}
	}

	l.Info("change captured", slog.String("id", id))

	event := &gen.UserCreatedEvent{
		Id: id,
	}

	val, err := proto.Marshal(event)
	if err != nil {
		logger.Error("failed to marshal event",
			slog.String("id", id),
			slog.String("error", err.Error()),
		)
		return []gokafka.Message{}
	}

	fngpnt := ck.MessageTypeHeaderValue(event)

	return []gokafka.Message{
		{
			Headers: []protocol.Header{
				{
					Key:   ck.MessageTypeHeaderKey,
					Value: []byte(fngpnt),
				},
			},
			Key:   []byte(id),
			Value: val,
		},
	}
}
