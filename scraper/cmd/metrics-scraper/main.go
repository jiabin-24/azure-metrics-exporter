package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/webdevops/azure-metrics-exporter/scraper/internal/scraper"
)

func main() {
	configPath := flag.String("config.file", "scraper.yaml", "Path to static scrape job YAML")
	listenAddress := flag.String("web.listen-address", ":8081", "Health server listen address")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *configPath, *listenAddress, logger); err != nil {
		logger.Error("metrics scraper stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, configPath, address string, logger *slog.Logger) (resultErr error) {
	cfg, err := scraper.LoadConfig(configPath)
	if err != nil {
		return err
	}
	sink := scraper.DiscardSink(logger)
	if cfg.EventHub != nil {
		var closeProducer func(context.Context) error
		sink, closeProducer, err = scraper.NewEventHubSink(*cfg.EventHub, logger)
		if err != nil {
			return err
		}
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := closeProducer(closeCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("close Event Hub producer: %w", err))
			}
		}()
	} else {
		logger.Warn("Event Hub is not configured; collected batches will be discarded")
	}
	worker := scraper.New(cfg, logger, sink)
	defer worker.Client.CloseIdleConnections()
	if err := worker.ResolveWorkspaces(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cluster *scraper.Cluster
	if cfg.Cluster != nil {
		cluster, err = scraper.NewCluster(ctx, cfg, logger)
		if err != nil {
			return err
		}
		defer func() {
			if err := cluster.Close(); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("close memberlist: %w", err))
			}
		}()
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for health checks: %w", err)
	}
	mux := http.NewServeMux()
	for _, path := range []string{"/healthz", "/readyz"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
			if path == "/readyz" && cluster != nil && !cluster.Ready() {
				http.Error(w, "Waiting for initial cluster join", http.StatusServiceUnavailable)
				return
			}
			if _, err := fmt.Fprintln(w, "Ok"); err != nil {
				logger.Error("write health response", "error", err)
			}
		})
	}
	server := &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: time.Minute,
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.Cluster = cluster
		worker.Run(ctx)
	}()
	logger.Info("scraper started",
		"jobs", len(cfg.Jobs), "health_address", listener.Addr().String())
	select {
	case <-ctx.Done():
	case err = <-serverErrors:
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	<-workerDone
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("health server: %w", err)
	}
	if shutdownErr != nil {
		return fmt.Errorf("shutdown health server: %w", shutdownErr)
	}
	return nil
}
