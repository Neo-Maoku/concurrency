package concurrency

import (
	"fmt"
	"github.com/shirou/gopsutil/v3/cpu"
	"log"
	"time"
)

func (c *Concurrency) logDebug(format string, v ...interface{}) {
	if c.LogLevel >= 2 {
		if v == nil {
			fmt.Println(format)
		} else {
			fmt.Println(fmt.Sprintf(format, v...))
		}
	}
}

func (c *Concurrency) logInfo(format string, v ...interface{}) {
	if c.LogLevel >= 1 {
		if v == nil {
			fmt.Println(format)
		} else {
			fmt.Println(fmt.Sprintf(format, v...))
		}
	}
}

func (c *Concurrency) logFault(format string, v ...interface{}) {
	if v == nil {
		log.Fatal(format)
	} else {
		log.Fatal(fmt.Sprintf(format, v...))
	}
}

func (c *Concurrency) initProgress(taskNum int) (result *[]int) {
	result = &[]int{}
	for i := 1; i <= 10; i++ {
		*result = append(*result, taskNum * i / 10)
	}

	return
}

func (c *Concurrency) progressPrint(taskFinishNum int, progress *[]int) {
	for i, value := range *progress {
		if value == taskFinishNum {
			if i+1 == 10 {
				c.logInfo(fmt.Sprintf("(%s) completed at %s, Takes %s times", c.taskName, time.Now().Format("15:04:05"), time.Now().Sub(c.startTime)))
			} else {
				c.logInfo(fmt.Sprintf("(%s) progress: %d/%d percentage: %d%%", c.taskName, taskFinishNum, (*progress)[9], (i+1)*10))
			}
			break
		}
	}
}

// 获取CPU占用率
func getCpuPercent() float64 {
	percent, _ := cpu.Percent(time.Second, false)
	if len(percent) > 0 {
		return percent[0]
	}
	return 0
}