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

func (s *scheduler) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.stopAll()
			return
		case cmd := <-s.cmdCh:
			s.handleCmd(ctx, cmd)
		}
	}
}

func (s *scheduler) spawnTicker(ctx context.Context, t domain.Target) {
	tickCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(time.Duration(t.CheckInterval) * time.Minute)
	s.target[t.ID] = &domain.TickerEntry{Ticker: ticker, Cancel: cancel}

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-tickCtx.Done():
				return
			case <-ticker.C:
				s.pool.Submit(worker.Job[domain.CheckJob]{
					Payload: domain.CheckJob{Target: t},
				})
			}
		}
	}()
}

func (s *scheduler) stopAll() {
	for _, entry := range s.target {
		entry.Cancel()
		entry.Ticker.Stop()
	}
}

func (s *scheduler) handleCmd(ctx context.Context, cmd domain.SchedulerCmd) {
	switch cmd.Type {
	case domain.CmdAdd:
		if cmd.Target != nil {
			s.spawnTicker(ctx, *cmd.Target)
		}
	case domain.CmdRemove:
		if entry, ok := s.target[cmd.TargetID]; ok {
			entry.Cancel()
			entry.Ticker.Stop()
			delete(s.target, cmd.TargetID)
		}
	case domain.CmdUpdateInterval:
		if entry, ok := s.target[cmd.TargetID]; ok {
			entry.Cancel()
			entry.Ticker.Stop()
			delete(s.target, cmd.TargetID)
		}
		if cmd.Target != nil {
			s.spawnTicker(ctx, *cmd.Target)
		}
	}
}
