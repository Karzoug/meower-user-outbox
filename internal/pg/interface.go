package pg

import (
	"context"

	"github.com/Karzoug/meower-user-outbox/internal/entity"
)

type producer interface {
	Produce(ctx context.Context, events []entity.UserChangedEvent) error
}
