package batchpipe

import "sync"

type Switch struct {
	ch chan struct{}
	mu sync.RWMutex
}

func NewSwitch() *Switch {
	return &Switch{ch: make(chan struct{})}
}

func (s *Switch) WaitOn() chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ch
}

func (s *Switch) IsOn() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

func (s *Switch) On() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case <-s.ch: // already ON
	default: // OFF
		close(s.ch)
	}
}

func (s *Switch) Off() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.ch: // ON
		s.ch = make(chan struct{})
	default:
	}
}
