package main

import (
	"fmt"
	"math"
	"time"

	batchpipe "github.com/Caviarion/BatchPipe"
	"github.com/sirupsen/logrus"
)

func timeFunc(i int) time.Duration {
	return time.Duration(time.Millisecond) * time.Duration(math.Round(math.Sqrt(float64(i))))
}

func main() {
	ppl := batchpipe.NewBatchPipeline()
	for i := 0; i < 5; i++ {
		node := batchpipe.NewBatchDelayNode(fmt.Sprintf("node-%d", i), 16, time.Millisecond*100, timeFunc)
		ppl.AddNode(-1, node)
	}
	ppl.StartPipeline()
	output := ppl.GetOutput()
	go func() {
		for {
			task := batchpipe.NewTimerTask()
			task.Start()
			ppl.InputFrom(0, task)
			time.Sleep(time.Millisecond * 10)
		}
	}()
	for {
		if v, ok := <-output; ok {
			task := v.(*batchpipe.TimerTask)
			task.Finish()
			if t, err := task.CostTime(); err != nil {
				logrus.Errorln(err)
			} else {
				logrus.Infoln(t)
			}
		} else {
			break
		}
	}
	ppl.ShutdownPipeline()
}
