package batchpipe

import (
	"fmt"
	"sync"
)

type Pipeline interface {
	StartPipeline()
	ShutdownPipeline()
	InputFrom(int, interface{}) error
	GetOutput() chan interface{}
	GetNodes() []CompNode
	GetNode(int) CompNode
	AddNode(int, CompNode) error
	DeleteNode(int)
}

type BatchPipeline struct {
	nodes   []CompNode
	started chan struct{}

	output chan interface{}
	mu     sync.RWMutex

	running *Switch
}

func NewBatchPipeline() *BatchPipeline {
	return &BatchPipeline{
		output:  make(chan interface{}),
		running: NewSwitch(),
	}
}

func (p *BatchPipeline) StartPipeline() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var prevOutput chan interface{}
	for _, node := range p.nodes {
		if prevOutput != nil {
			node.SetInput(prevOutput)
		}
		node.Execute()
		prevOutput = node.GetOutput()
	}
	go func() {
		<-p.running.WaitOn()
		for v := range prevOutput {
			if !p.running.IsOn() {
				break
			}
			p.output <- v
		}
		close(p.output)
	}()
	p.running.On()
}

func (p *BatchPipeline) ShutdownPipeline() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, node := range p.nodes {
		node.Shutdown()
	}
	p.running.Off()
}

func (p *BatchPipeline) InputFrom(index int, value interface{}) error {
	p.mu.RLock()
	if index >= len(p.nodes) {
		p.mu.RUnlock()
		return fmt.Errorf("Index out of range: %d[%d]", index, len(p.nodes))
	}
	node := p.nodes[index]
	p.mu.RUnlock()
	return node.PutInput(value)
}

func (p *BatchPipeline) GetOutput() chan interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.output
}

func (p *BatchPipeline) GetNodes() []CompNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nodes
}

func (p *BatchPipeline) GetNode(index int) CompNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if index >= len(p.nodes) || index < 0 {
		return nil
	}
	return p.nodes[index]
}

func (p *BatchPipeline) AddNode(index int, node CompNode) error {
	if p.running.IsOn() {
		return fmt.Errorf("Pipeline still running")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if index >= len(p.nodes) {
		return fmt.Errorf("Index %d out of range (,%d)", index, len(p.nodes))
	}
	if index < 0 {
		p.nodes = append(p.nodes, node)
	} else {
		p.nodes = append(append(p.nodes[:index], node), p.nodes[index:]...)
	}
	return nil
}

func (p *BatchPipeline) DeleteNode(index int) error {
	if p.running.IsOn() {
		return fmt.Errorf("Pipeline still running")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if index >= len(p.nodes) || index < 0 {
		return fmt.Errorf("Index %d out of range [0,%d)", index, len(p.nodes))
	}
	p.nodes = append(p.nodes[:index], p.nodes[index+1:]...)
	return nil
}
