package batchpipe

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

type TimerTask struct {
	started    chan struct{}
	finished   chan struct{}
	startTime  time.Time
	finishTime time.Time
}

func NewTimerTask() *TimerTask {
	return &TimerTask{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
	}
}

func (t *TimerTask) Start() {
	select {
	case <-t.started:
	default:
		close(t.started)
		t.startTime = time.Now()
	}
}

func (t *TimerTask) Finish() {
	select {
	case <-t.started:
		select {
		case <-t.finished:
		default:
			close(t.finished)
			t.finishTime = time.Now()
		}
	default:
		logrus.Warnln("Task not start yet")
	}
}

func (t *TimerTask) CostTime() (time.Duration, error) {
	select {
	case <-t.finished:
		return t.finishTime.Sub(t.startTime), nil
	default:
		return time.Duration(0), fmt.Errorf("Task not finished yet")
	}
}
