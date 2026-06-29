package usecase

import (
	"context"
	"sitepulse/internal/domain"
	"sitepulse/internal/repository"
	"sitepulse/internal/worker"
	"sync"
	"time"
)

type SchedulerUseCase interface {
	Run(ctx context.Context)
	AddTarget(ctx context.Context, target domain.Target) error
	RemoveTarget(id int64) error
	UpdateInterval(id int64, d time.Duration) error
}
type WorkerPool interface {
	Submit(job worker.Job[domain.CheckJob]) bool
}
type scheduler struct {
	target map[int64]*domain.TickerEntry
	mu     sync.RWMutex
	cmdCh  chan domain.SchedulerCmd
	pool   WorkerPool
	repo   repository.CheckResultRepository
}
