package concurrency

import (
	"context"
	"github.com/panjf2000/ants/v2"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Range struct {
	LValue int
	RValue int
}

type goroutineParam struct {
	Param         interface{}
	Result        *[]interface{}
	Wg            *sync.WaitGroup
	TaskFunc      func(interface{}, *[]interface{})
	TaskTimeSleep interface{}
	TaskTimeOut   int
}

type TaskParam struct {
	ConcurrencyParams []interface{}
	TaskFunc          func(interface{}, *[]interface{})
	TaskName          string
}

type Concurrency struct {
	TaskGroupCount      interface{}
	TaskTimeSleep       interface{}
	TaskGroupTimeSleep  interface{}
	GoroutineNumLimit   int
	GoroutineNum        int
	SysMonitor          bool
	TaskTimeOut         int
	LogLevel            int
	isGroupCountInt     bool
	isGroupTimeSleepInt bool
	isTaskTimeSleepInt  bool
	taskStopSleep       bool
	startTime           time.Time
	progressMap         map[int]bool
	taskName            string
}

func (c *Concurrency) perfMonitor(p *ants.PoolWithFunc) {
	stopMem := false
	maxGoroutineNum := c.GoroutineNumLimit
	taskName := c.taskName

	for {
		if p.IsClosed() {
			break
		}

		if !c.taskStopSleep {
			length := p.Cap()
			if stopMem == false {
				mem := getMemPercent()
				c.logDebug("(%s) Concurrency mem:%f", taskName, mem)

				if mem > 80 {
					goroutineCount := int(math.Ceil(float64(length) / 10.0 * 9))
					if goroutineCount < 1 {
						goroutineCount = 1
					}
					p.Tune(goroutineCount)
					c.logDebug("(%s) Concurrency mem dec, goroutinePoolCount:%d", taskName, goroutineCount)
				} else if mem < 70 {
					goroutineCount := int(math.Ceil(float64(length) / 10.0 * 11))
					if goroutineCount > maxGoroutineNum {
						goroutineCount = maxGoroutineNum
					}
					p.Tune(goroutineCount)

					c.logDebug("(%s) Concurrency mem inc, goroutinePoolCount:%d", taskName, goroutineCount)
				}
			}

			cpu := getCpuPercent()
			c.logDebug("(%s) Concurrency cpu:%f", taskName, cpu)

			if cpu != 0 {
				if cpu > 70 {
					stopMem = true
					goroutineCount := int(math.Floor(float64(length) / 10.0 * 9))
					if goroutineCount < 1 {
						goroutineCount = 1
					}
					p.Tune(goroutineCount)

					c.logDebug("(%s) Concurrency cpu dec, goroutinePoolCount:%d", taskName, goroutineCount)
				} else if cpu > 60 {
					stopMem = true
				} else {
					GoroutineCount := int(math.Ceil(float64(length) / 10.0 * 11))

					if GoroutineCount > maxGoroutineNum {
						GoroutineCount = maxGoroutineNum
					}
					p.Tune(GoroutineCount)

					c.logDebug("(%s) Concurrency cpu inc, goroutinePoolCount:%d", taskName, GoroutineCount)
				}
			}
		}
	}
}

func (c *Concurrency) task(taskInterface interface{}) {
	taskObj, ok := taskInterface.(goroutineParam)

	if ok {
		var taskTimeSleepResult int

		if c.isTaskTimeSleepInt {
			taskTimeSleepResult = taskObj.TaskTimeSleep.(int)
		} else {
			taskTimeSleepResult = rand.Intn(taskObj.TaskTimeSleep.(Range).RValue-taskObj.TaskTimeSleep.(Range).LValue) + taskObj.TaskTimeSleep.(Range).LValue
		}
		if taskTimeSleepResult != 0 {
			time.Sleep(time.Duration(taskTimeSleepResult) * time.Millisecond)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(taskObj.TaskTimeOut))
		defer cancel()

		go func(ctx context.Context, cancel context.CancelFunc) {
			select {
			case <-ctx.Done():
				return
			default:
				defer cancel()
				taskObj.TaskFunc(taskObj.Param, taskObj.Result)
			}
		}(ctx, cancel)

		<-ctx.Done()

		taskObj.Wg.Done()
	}
}

func (c *Concurrency) Run(taskParam TaskParam) (results []interface{}) {
	var wg = &sync.WaitGroup{}
	var taskFinishNum = 0
	var taskTotalNum = len(taskParam.ConcurrencyParams)

	_, c.isGroupCountInt = c.TaskGroupCount.(int)
	_, c.isGroupTimeSleepInt = c.TaskGroupTimeSleep.(int)
	_, c.isTaskTimeSleepInt = c.TaskTimeSleep.(int)
	c.taskName = taskParam.TaskName

	p, err := ants.NewPoolWithFunc(c.GoroutineNum, c.task, ants.WithPreAlloc(false))
	if err != nil {
		c.logFault("NewPoolWithFunc fail")
	}
	defer p.Release()

	c.initProgress()
	c.startTime = time.Now()

	if c.SysMonitor {
		go c.perfMonitor(p)
	}

	taskGroupCountResult, taskGroupTimeSleepResult := c.switchGoroutineVaule()

	wg.Add(taskTotalNum)
	var count = 0
	for _, param := range taskParam.ConcurrencyParams {
		taskFinishNum++

		if taskTotalNum > 10 && taskFinishNum != taskTotalNum {
			c.progressPrint(taskParam.TaskName, taskFinishNum, taskTotalNum)
		}

		if count == taskGroupCountResult && taskGroupCountResult != 0 {
			c.taskStopSleep = true
			count = 0
			if taskGroupTimeSleepResult != 0 {
				time.Sleep(time.Duration(taskGroupTimeSleepResult) * time.Millisecond)
			}

			taskGroupCountResult, taskGroupTimeSleepResult = c.switchGoroutineVaule()

			c.taskStopSleep = false
		}

		if err := p.Invoke(goroutineParam{Param: param, Result: &results, Wg: wg, TaskFunc: taskParam.TaskFunc, TaskTimeSleep: c.TaskTimeSleep, TaskTimeOut: c.TaskTimeOut}); err != nil {
			c.logFault("newPoolWithFunc invoke fail")
		}

		count++
	}
	wg.Wait()

	c.progressPrint(taskParam.TaskName, taskTotalNum, taskTotalNum)

	return results
}

func (c *Concurrency) switchGoroutineVaule() (taskGroupCountResult, taskGroupTimeSleepResult int) {
	rand.Seed(time.Now().UnixNano())
	if c.isGroupCountInt {
		taskGroupCountResult = c.TaskGroupCount.(int)
	} else {
		taskGroupCountResult = rand.Intn(c.TaskGroupCount.(Range).RValue-c.TaskGroupCount.(Range).LValue) + c.TaskGroupCount.(Range).LValue
	}

	if c.isGroupTimeSleepInt {
		taskGroupTimeSleepResult = c.TaskGroupTimeSleep.(int)
	} else {
		taskGroupTimeSleepResult = rand.Intn(c.TaskGroupTimeSleep.(Range).RValue-c.TaskGroupTimeSleep.(Range).LValue) + c.TaskGroupTimeSleep.(Range).LValue
	}
	return
}

func New() *Concurrency {
	return NewConcurrency(Concurrency{LogLevel: 1, SysMonitor: true})
}

func NewConcurrency(c Concurrency) *Concurrency {
	var tmp = Concurrency{
		TaskGroupCount:     1000,
		TaskTimeSleep:      Range{LValue: 0, RValue: 300},
		TaskGroupTimeSleep: 1500,
		GoroutineNumLimit:  5000,
		GoroutineNum:       3000,
		SysMonitor:         false,
		TaskTimeOut:        15000,
		LogLevel:           0,
	}

	if c.TaskGroupCount != nil {
		tmp.TaskGroupCount = c.TaskGroupCount
	}
	if c.TaskTimeSleep != nil {
		tmp.TaskTimeSleep = c.TaskTimeSleep
	}
	if c.TaskGroupTimeSleep != nil {
		tmp.TaskGroupTimeSleep = c.TaskGroupTimeSleep
	}
	if c.GoroutineNumLimit != 0 {
		tmp.GoroutineNumLimit = c.GoroutineNumLimit
	}
	if c.GoroutineNum != 0 {
		tmp.GoroutineNum = c.GoroutineNum
	}
	if c.TaskTimeOut != 0 {
		tmp.TaskTimeOut = c.TaskTimeOut
	}
	tmp.LogLevel = c.LogLevel
	tmp.SysMonitor = c.SysMonitor

	return &tmp
}
