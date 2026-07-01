package domain

import (
	"context"
	"time"
)

type CmdType int

const (
	CmdAdd CmdType = iota
	CmdRemove
	CmdUpdateInterval
)

type SchedulerCmd struct {
	Type     CmdType
	TargetID int64
	Interval time.Duration
	Target   *Target
}

type TickerEntry struct {
	Ticker *time.Ticker
	Cancel context.CancelFunc
}
