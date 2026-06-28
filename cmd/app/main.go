package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"sitepulse/internal/infrastructure/netclient"
	"sitepulse/internal/repository/postgres"
	"sitepulse/internal/usecase"
	"sitepulse/internal/worker"

	_ "github.com/lib/pq"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}

	targetRepo := postgres.NewTargetRepo(db)
	checkResultRepo := postgres.NewCheckResultRepo(db)

	checker := netclient.NewChecker(logger)

	uc := usercase.NewMonitoringUseCase(targetRepo, checkResultRepo, checker, nil, logger)

	pool := worker.NewPool(
		10, 50,
		uc.HandleCheckJob,
		logger,
	)
	uc.SetPool(pool)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool.Start(ctx)

	if err := uc.EnqueueAllTargets(ctx); err != nil {
		logger.Error("initial enqueue failed", "error", err)
	}

	<-ctx.Done()
	logger.Info("shutdown signal received, waiting for workers...")

	pool.Wait()
	logger.Info("all workers done, goodbye")
}
