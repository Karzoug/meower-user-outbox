package pg

import "time"

type Config struct {
	ReservedTimeout    time.Duration `env:"RESERVED_TIMEOUT" envDefault:"1m"`
	BatchSize          int           `env:"BATCH_SIZE" envDefault:"100"`
	CheckInterval      time.Duration `env:"CHECK_INTERVAL" envDefault:"1s"`
	MaxErrorsCount     int           `env:"MAX_ERRORS_COUNT" envDefault:"5"`
	HandleBatchTimeout time.Duration `env:"HANDLE_BATCH_TIMEOUT" envDefault:"5s"`
}
