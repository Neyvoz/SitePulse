package usercase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sitepulse/internal/domain"
	"sitepulse/internal/repository"
	"sitepulse/internal/worker"
)

// Checker — интерфейс сетевого движка из Недели 2.
type Checker interface {
	Check(ctx context.Context, target domain.Target) (domain.CheckResult, error)
}

type MonitoringUseCase struct {
	targetRepo      repository.TargetRepository
	checkResultRepo repository.CheckResultRepository
	checker         Checker
	pool            *worker.Pool[domain.CheckJob]
	logger          *slog.Logger
}

func NewMonitoringUseCase(
	targetRepo repository.TargetRepository,
	checkResultRepo repository.CheckResultRepository,
	checker Checker,
	pool *worker.Pool[domain.CheckJob],
	logger *slog.Logger,
) *MonitoringUseCase {
	return &MonitoringUseCase{
		targetRepo:      targetRepo,
		checkResultRepo: checkResultRepo,
		checker:         checker,
		pool:            pool,
		logger:          logger,
	}
}

func (uc *MonitoringUseCase) SetPool(pool *worker.Pool[domain.CheckJob]) {
	uc.pool = pool
}

func (uc *MonitoringUseCase) EnqueueAllTargets(ctx context.Context) error {
	targets, err := uc.targetRepo.GetAllActive(ctx)
	if err != nil {
		return fmt.Errorf("monitoring: get active targets: %w", err)
	}

	for _, t := range targets {
		job := worker.Job[domain.CheckJob]{
			Payload: domain.CheckJob{Target: t},
		}
		if !uc.pool.Submit(job) {
			uc.logger.Warn("monitoring: job dropped for target",
				"target_id", t.ID,
				"url", t.URL,
			)
		}
	}
	return nil
}

func (uc *MonitoringUseCase) HandleCheckJob(ctx context.Context, job worker.Job[domain.CheckJob]) {
	target := job.Payload.Target

	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result domain.CheckResult

	checkedResult, err := uc.checker.Check(checkCtx, target)
	if err != nil {
		uc.logger.Error("monitoring: check failed",
			"target_id", target.ID,
			"url", target.URL,
			"error", err,
		)
		result = domain.CheckResult{
			TargetID:     target.ID,
			CheckedAt:    time.Now(),
			IsUp:         false,
			ErrorMessage: err.Error(),
		}
	} else {
		result = checkedResult
	}

	saveCtx, saveCancel := context.WithTimeout(ctx, 5*time.Second)
	defer saveCancel()

	if err := uc.checkResultRepo.Save(saveCtx, result); err != nil {
		uc.logger.Error("monitoring: save result failed",
			"target_id", target.ID,
			"error", err,
		)
	}
}
