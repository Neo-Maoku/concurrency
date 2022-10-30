package concurrency

import (
	"context"
	"fmt"
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

type ParamsFunc interface {
	ParamsCreate(ch chan interface{}, taskTotal chan int)
}

//并发执行任务的单个参数结构
type goroutineParam struct {
	Param         interface{}                       //协程执行需要的参数
	Result        *[]interface{}                    //协程执行结果
	Wg            *sync.WaitGroup                   //协程同步信号量
	TaskFunc      func(interface{}, *[]interface{}) //要并发执行的任务函数
	TaskTimeSleep Range                             //错峰执行的时间范围，单位为ms
	TaskTimeOut   int                               //协程执行超时时间，单位为ms
}

//	需要开启CPU监控，该参数才生效
type CPUParam struct {
	CPULimit Range //cpu监控的区间，小于等于LValue就上涨，大于RValue就下跌
	Percent  int   //上涨和下跌的百分比
	FixedNum int   //固定的上涨和下跌值
}

type Task struct {
	TaskParamsFunc ParamsFunc                        //任务参数生成的接口函数
	TaskParams     []interface{}                     //任务参数集合
	TaskFunc       func(interface{}, *[]interface{}) //要执行的任务函数体
	TaskName       string                            //任务名
}

type Concurrency struct {
	TaskGroupCount      interface{} //每组协程数量，可以是固定值或区间。默认值为1000
	TaskTimeSleep       Range       //错峰执行的时间范围，单位为ms。默认值为0-300
	TaskGroupTimeSleep  interface{} //每组协程数执行完后的暂停时间，可以是固定值或区间，单位为ms，默认值为1500.当TaskGroupCount大于0时才生效
	GoroutineNumLimit   int         //最大并发协程数，默认值为5000
	GoroutineNum        int         //初始并发协程数，小于等于GoroutineNumLimit，默认值为3000
	SysMonitor          bool        //是否需要监控cpu占用率，默认是开启的
	TaskTimeOut         int         //协程执行超时时间，单位为ms,默认值为15000
	LogLevel            int         //控制台打印等级，0:不输出任何信息，1:输出任务进度信息，>=2:输出所有信息。默认值为1
	CPUInfo             CPUParam    //监控CPU的参数，默认Range为[60-70], 变动百分比为10
	isGroupCountInt     bool        //判断TaskGroupCount类型
	isGroupTimeSleepInt bool        //判断TaskGroupTimeSleep类型
	taskStopSleep       bool        //判断是否处于暂停时间，用于和cpu监控联动
	startTime           time.Time   //记录任务完成时，耗时情况
	taskName            string      //任务名
	taskTotalNum        int
	taskFinishNum       int
	taskRunning         bool
	curGoroutineNum		int
}

//性能监控（cpu占用率），当cpu>70，线程池在原有基础上降低10%。当cpu<60,线程池在原有基础上增加10%。
func (c *Concurrency) perfMonitor(p *ants.PoolWithFunc) {
	maxGoroutineNum := c.GoroutineNumLimit
	taskName := c.taskName
	cpuInfo := c.CPUInfo
	isFixed := false
	if cpuInfo.Percent == 0 {
		isFixed = true
	}

	for {
		if p.IsClosed() {
			break
		}

		if !c.taskStopSleep {
			c.curGoroutineNum = p.Cap()
			cpu := int(getCpuPercent())

			if cpu != 0 {
				var goroutineCount int

				if cpu > cpuInfo.CPULimit.RValue {
					if isFixed {
						goroutineCount = c.curGoroutineNum - cpuInfo.FixedNum
					} else {
						goroutineCount = int(math.Floor(float64(c.curGoroutineNum*(100-cpuInfo.Percent)) / 100))
					}
					if goroutineCount < 1 {
						goroutineCount = 1
					}
					p.Tune(goroutineCount)

					c.logDebug("(%s) Concurrency cpu(%d) > %d, goroutine count dec, goroutinePoolCount:%d", taskName, cpu, cpuInfo.CPULimit.RValue, goroutineCount)
				} else if cpu <= cpuInfo.CPULimit.LValue {
					if isFixed {
						goroutineCount = c.curGoroutineNum + cpuInfo.FixedNum
					} else {
						goroutineCount = int(math.Floor(float64(c.curGoroutineNum*(100+cpuInfo.Percent)) / 100))
					}

					if goroutineCount > maxGoroutineNum {
						goroutineCount = maxGoroutineNum
					}
					p.Tune(goroutineCount)

					c.logDebug("(%s) Concurrency cpu(%d) <= %d, goroutine count inc, goroutinePoolCount:%d", taskName, cpu, cpuInfo.CPULimit.LValue, goroutineCount)
				}
			}
		}
	}
}

func (c *Concurrency) task(taskInterface interface{}) {
	taskObj, ok := taskInterface.(goroutineParam)

	if ok {
		if taskObj.TaskTimeSleep.RValue != 0 {
			taskTimeSleepResult := rand.Intn(taskObj.TaskTimeSleep.RValue-taskObj.TaskTimeSleep.LValue) + taskObj.TaskTimeSleep.LValue
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

func (c *Concurrency) Run(task Task) (results []interface{}) {
	var wg = &sync.WaitGroup{}
	var count = 0
	c.taskFinishNum = 0
	c.taskTotalNum = len(task.TaskParams)

	_, c.isGroupCountInt = c.TaskGroupCount.(int)
	_, c.isGroupTimeSleepInt = c.TaskGroupTimeSleep.(int)
	c.taskName = "Task"
	if task.TaskName != "" {
		c.taskName = task.TaskName
	}
	c.startTime = time.Now()

	p, err := ants.NewPoolWithFunc(c.GoroutineNum, c.task, ants.WithPreAlloc(false))
	if err != nil {
		c.logFault("NewPoolWithFunc fail")
	} else {
		defer p.Release()
	}

	if c.SysMonitor {
		go c.perfMonitor(p)
	}

	taskGroupCountResult, taskGroupTimeSleepResult := c.switchGoroutineValue()

	var ch = make(chan interface{}, 10000)
	if c.taskTotalNum == 0 {
		if task.TaskParamsFunc != nil {
			var taskTotal = make(chan int)

			go task.TaskParamsFunc.ParamsCreate(ch, taskTotal)

			c.taskTotalNum = <-taskTotal
		}
	} else {
		close(ch)
	}

	if c.taskTotalNum == 0 {
		c.logInfo(fmt.Sprintf("(%s) Warn: TaskNum is Zero", c.taskName))
		return nil
	}

	var isShowProgress = false
	if c.taskTotalNum/taskGroupCountResult >= 10 {
		isShowProgress = true
	}

	progress := c.initProgress(c.taskTotalNum)

	wg.Add(c.taskTotalNum)

	workFunc := func(param interface{}) {
		if isShowProgress {
			c.progressPrint(progress)
		}

		if count == taskGroupCountResult && taskGroupCountResult != 0 {
			c.taskStopSleep = true
			count = 0
			if taskGroupTimeSleepResult != 0 {
				time.Sleep(time.Duration(taskGroupTimeSleepResult) * time.Millisecond)
			}

			taskGroupCountResult, taskGroupTimeSleepResult = c.switchGoroutineValue()

			c.taskStopSleep = false
		}

		if err := p.Invoke(goroutineParam{Param: param, Result: &results, Wg: wg, TaskFunc: task.TaskFunc, TaskTimeSleep: c.TaskTimeSleep, TaskTimeOut: c.TaskTimeOut}); err != nil {
			c.logFault("newPoolWithFunc invoke fail")
		}

		c.taskFinishNum++
		count++
	}

	c.taskRunning = true

	if len(task.TaskParams) > 0 {
		for _, param := range task.TaskParams {
			workFunc(param)
		}
	} else {
		for {
			param, ok := <-ch
			if !ok {
				break
			}
			workFunc(param)
		}
	}

	wg.Wait()

	c.progressPrint(progress)
	c.taskRunning = false

	return results
}

func (c *Concurrency) PrintTaskProgress() {
	if c.taskRunning {
		c.logInfo(fmt.Sprintf("(%s) curGoroutine:%d progress: %d/%d percentage: %s%%", c.taskName, c.curGoroutineNum, c.taskFinishNum, c.taskTotalNum, fmt.Sprintf("%0.1f", float32(c.taskFinishNum)/float32(c.taskTotalNum)*100)))
	}
}

func (c *Concurrency) switchGoroutineValue() (taskGroupCountResult, taskGroupTimeSleepResult int) {
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
	return NewConcurrency(Concurrency{LogLevel: 1, SysMonitor: true, TaskTimeSleep: Range{LValue: 0, RValue: 300}})
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
		CPUInfo:            CPUParam{CPULimit: Range{LValue: 60, RValue: 70}, Percent: 10},
	}

	if c.TaskGroupCount != nil {
		tmp.TaskGroupCount = c.TaskGroupCount
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
	if c.CPUInfo.CPULimit.RValue != 0 {
		tmp.CPUInfo = c.CPUInfo
	}

	tmp.LogLevel = c.LogLevel
	tmp.SysMonitor = c.SysMonitor
	tmp.TaskTimeSleep = c.TaskTimeSleep

	return &tmp
}
