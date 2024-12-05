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
			var event *gen.ChangedEvent

			switch msg.Type {
			case cdc.InsertMessage:
				event = userCreatedHandler(msg, logger)
			case cdc.DeleteMessage:
				event = userDeletedHandler(msg, logger)
			default:
				return []gokafka.Message{}
			}

			if nil == event {
				return []gokafka.Message{}
			}

			val, err := proto.Marshal(event)
			if err != nil {
				logger.Error("failed to marshal event",
					slog.String("id", event.Id),
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
					Key:   []byte(event.Id),
					Value: val,
				},
			}
		}

		return []gokafka.Message{}
	}
}

func userCreatedHandler(msg *cdc.Message, logger *slog.Logger) *gen.ChangedEvent {
	l := logger.With(
		slog.String("table", msg.TableName),
		slog.String("table operation", string(msg.Type)),
	)

	id, ok := msg.NewData["id"].(string)
	if !ok {
		l.Error("invalid id format",
			slog.Any("id", msg.NewData["id"]),
		)
		return nil
	}

	l.Info("event received", slog.String("id", id))

	return &gen.ChangedEvent{
		Id:         id,
		ChangeType: gen.ChangeType_CHANGE_TYPE_CREATED,
	}
}

func userDeletedHandler(msg *cdc.Message, logger *slog.Logger) *gen.ChangedEvent {
	l := logger.With(
		slog.String("table", msg.TableName),
		slog.String("table operation", string(msg.Type)),
	)

	id, ok := msg.OldData["id"].(string)
	if !ok {
		l.Error("invalid id format",
			slog.Any("id", msg.NewData["id"]),
		)
		return nil
	}

	l.Info("event received", slog.String("id", id))

	return &gen.ChangedEvent{
		Id:         id,
		ChangeType: gen.ChangeType_CHANGE_TYPE_DELETED,
	}
}
