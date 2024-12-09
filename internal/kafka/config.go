package kafka

import (
	"time"
)

type Config struct {
	Brokers           string        `env:"BROKERS,notEmpty"`
	CloseTimeout      time.Duration `env:"CLOSE_TIMEOUT" envDefault:"10s"`
	EnableIdempotence bool          `env:"ENABLE_IDEMPOTENCE" envDefault:"false"`
}
