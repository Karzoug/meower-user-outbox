package app

import (
	"context"
	"runtime"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/Karzoug/meower-common-go/metric/prom"
	"github.com/Karzoug/meower-common-go/postgresql"

	"github.com/Karzoug/meower-user-outbox/internal/config"
	"github.com/Karzoug/meower-user-outbox/internal/kafka"
	kmetric "github.com/Karzoug/meower-user-outbox/internal/metric"
	"github.com/Karzoug/meower-user-outbox/internal/pg"
	"github.com/Karzoug/meower-user-outbox/pkg/buildinfo"
)

const (
	serviceName     = "UserOutbox"
	metricNamespace = "user_outbox"
	pkgName         = "github.com/Karzoug/meower-user-outbox"
	initTimeout     = 10 * time.Second
	shutdownTimeout = 10 * time.Second
)

var serviceVersion = buildinfo.Get().ServiceVersion

func Run(ctx context.Context, logger zerolog.Logger) error {
	cfg, err := env.ParseAs[config.Config]()
	if err != nil {
		return err
	}
	zerolog.SetGlobalLevel(cfg.LogLevel)

	logger.Info().
		Int("GOMAXPROCS", runtime.GOMAXPROCS(0)).
		Str("log level", cfg.LogLevel.String()).
		Msg("starting up")

	// set timeout for initialization
	ctxInit, closeCtx := context.WithTimeout(ctx, initTimeout)
	defer closeCtx()

	// set up meter
	shutdownMeter, err := prom.RegisterGlobal(ctxInit, serviceName, serviceVersion, metricNamespace)
	if err != nil {
		return err
	}
	defer doClose(shutdownMeter, logger)

	meter := otel.GetMeterProvider().Meter(pkgName, metric.WithInstrumentationVersion(serviceVersion))

	// custom kafka metrics
	rec, err := kmetric.NewKafkaRecorder(meter)
	if err != nil {
		return err
	}

	eventProducer, shutdownProducer, err := kafka.NewProducer(ctxInit, cfg.Kafka, rec, logger)
	if err != nil {
		return err
	}
	defer doClose(shutdownProducer, logger)

	db, err := postgresql.NewDB(ctxInit, cfg.PG)
	if err != nil {
		return err
	}
	defer doClose(db.Close, logger)

	eventConsumer := pg.NewEventConsumer(cfg.Consumer, db, eventProducer, logger)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return eventConsumer.Run(ctx)
	})
	eg.Go(func() error {
		return eventProducer.Run(ctx)
	})
	// run prometheus metrics http server
	eg.Go(func() error {
		return prom.Serve(ctx, cfg.PromHTTP, logger)
	})

	return eg.Wait()
}

func doClose(fn func(context.Context) error, logger zerolog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := fn(ctx); err != nil {
		logger.Error().
			Err(err).
			Msg("error closing")
	}
}
