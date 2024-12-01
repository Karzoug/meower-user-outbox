package app

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/caarlos0/env/v11"
	"golang.org/x/sync/errgroup"

	"github.com/Karzoug/meower-user-outbox/internal/config"
	"github.com/Karzoug/meower-user-outbox/internal/outbox"
)

const (
	initTimeout     = 10 * time.Second
	shutdownTimeout = 5 * time.Second
)

func Run(ctx context.Context) error {
	cfg, err := env.ParseAs[config.Config]()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout,
		&slog.HandlerOptions{
			Level: cfg.LogLevel,
		}),
	)

	logger.Info("starting up",
		slog.Int("GOMAXPROCS", runtime.GOMAXPROCS(0)),
		slog.String("log level", cfg.LogLevel.String()),
	)

	ctxInit, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()

	cnctr, shutdownCnctr, err := outbox.NewConnector(ctxInit, cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		ctxShutdown, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()

		if err := shutdownCnctr(ctxShutdown); err != nil {
			logger.Error(
				"connector close error",
				slog.String("error", err.Error()),
			)
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return cnctr.Run(ctx)
	})

	return eg.Wait()
}
