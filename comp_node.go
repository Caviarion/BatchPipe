package batchpipe

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type CompNode interface {
	GetName() string
	Execute()
	SetInput(chan interface{})
	PutInput(interface{}) error
	GetOutput() chan interface{}
	Shutdown()
}

type BatchDelayNode struct {
	name      string
	delayFunc func(int) time.Duration
	input     chan *TimerTask
	output    chan *TimerTask
	batchSize int
	batchWait time.Duration

	running *Switch

	mu sync.RWMutex
}

func NewBatchDelayNode(name string, batchSize int, batchWait time.Duration, calc func(int) time.Duration) *BatchDelayNode {
	if batchSize < 1 {
		batchSize = 1
	}
	return &BatchDelayNode{
		name:      name,
		delayFunc: calc,
		input:     make(chan *TimerTask, batchSize),
		output:    make(chan *TimerTask, batchSize),
		running:   NewSwitch(),
		batchSize: batchSize,
		batchWait: batchWait,
	}
}

func (b *BatchDelayNode) GetName() string {
	return b.name
}

func (b *BatchDelayNode) SetInput(input chan interface{}) {
	go func() {
		<-b.running.WaitOn()
		for {
			inp, ok := <-input
			if !ok {
				break
			}
			switch v := inp.(type) {
			case *TimerTask:
				b.input <- v
			default:
				logrus.Warnf("Invalid input type: %T", v)
			}
		}
	}()
}

func (b *BatchDelayNode) PutInput(input interface{}) error {
	if !b.running.IsOn() {
		return fmt.Errorf("BatchDelayNode[%s] not running", b.name)
	}
	switch v := input.(type) {
	case *TimerTask:
		b.input <- v
	default:
		return fmt.Errorf("Invalid input type: %T", v)
	}
	return nil
}

func (b *BatchDelayNode) GetOutput() chan interface{} {
	out := make(chan interface{})
	go func() {
		for v := range b.output {
			out <- v
			if !b.running.IsOn() {
				break
			}
		}
		close(out)
	}()
	return out
}

func (b *BatchDelayNode) Execute() {
	if b.running.IsOn() {
		return
	}
	var batch []*TimerTask
	timer := time.NewTimer(b.batchWait)
	emit := func(batch []*TimerTask) {
		timer.Reset(b.batchWait)
		b.mu.RLock()
		f := b.delayFunc
		b.mu.RUnlock()
		if f == nil {
			logrus.Panicf("Delay func for BatchDelayNode[%s] not set", b.name)
		}
		time.Sleep(f(len(batch)))
		for _, t := range batch {
			b.output <- t
		}
	}
	b.running.On()
	go func() {
		for b.running.IsOn() {
			inp := <-b.input
			batch = append(batch, inp)
			select {
			case <-timer.C:
				timer.Reset(b.batchWait)
				go emit(batch)
			default:
				if len(batch) == b.batchSize {
					timer.Reset(b.batchWait)
					go emit(batch)
				}
			}
		}
	}()
}

// Nodes after shutdown is not thread safe
func (b *BatchDelayNode) Shutdown() {
	b.running.Off()
}

func (b *BatchDelayNode) SetDelayFunc(f func(int) time.Duration) error {
	if f == nil {
		return fmt.Errorf("Nil delay func for BatchDelayNode[%s]", b.name)
	}
	b.mu.Lock()
	b.delayFunc = f
	b.mu.Unlock()
	return nil
}
