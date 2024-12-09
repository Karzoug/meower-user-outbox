package entity

import (
	"time"

	"github.com/rs/xid"
)

type ChangeType string

const (
	ChangeTypeCreate ChangeType = "create"
	ChangeTypeDelete ChangeType = "delete"
)

type UserChangedEvent struct {
	ID         int64      `db:"id"`
	ChangeType ChangeType `db:"change_type"`
	UserID     xid.ID     `db:"user_id"`
	CreatedAt  time.Time  `db:"created_at"`
}
